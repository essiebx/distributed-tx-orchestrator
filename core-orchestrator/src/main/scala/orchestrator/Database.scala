package orchestrator

import io.getquill._
import io.getquill.jdbczio.Quill
import zio._

import java.sql.SQLException

case class SagaLogRow(
    transactionId: String,
    state: String,
    timestamp: Long
)

// Repository using ZIO interface
trait SagaRepository {
  def insertLog(row: SagaLogRow): ZIO[Any, SQLException, Unit]
  def getLogs(transactionId: String): ZIO[Any, SQLException, List[SagaLogRow]]
}

case class SagaRepositoryLive(quill: Quill.Postgres[SnakeCase.type]) extends SagaRepository {
  import quill._

  override def insertLog(row: SagaLogRow): ZIO[Any, SQLException, Unit] =
    run(querySchema[SagaLogRow]("saga_log").insertValue(lift(row))).unit

  override def getLogs(transactionId: String): ZIO[Any, SQLException, List[SagaLogRow]] =
    run(querySchema[SagaLogRow]("saga_log").filter(_.transactionId == lift(transactionId)).sortBy(_.timestamp)(Ord.asc))
}

object SagaRepositoryLive {
  val layer: URLayer[Quill.Postgres[SnakeCase.type], SagaRepository] = ZLayer.fromFunction(SagaRepositoryLive.apply _)
}
