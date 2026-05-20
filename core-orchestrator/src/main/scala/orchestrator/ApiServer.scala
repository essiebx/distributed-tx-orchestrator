package orchestrator

import zio._
import zio.http._
import zio.stream._

object ApiServer {

  def routes: HttpApp[Engine with Hub[SagaLogRow]] =
    Routes(
      Method.POST / "api" / "transactions" -> handler { (req: Request) =>
        for {
          engine <- ZIO.service[Engine]
          id = java.util.UUID.randomUUID().toString.take(8)
          _ <- engine.startSaga(id, 100.0) // Demo amount
        } yield Response.json(s"""{"id":"$id"}""")
      },

      Method.GET / "api" / "transactions" / "stream" -> handler { (req: Request) =>
        for {
          hub <- ZIO.service[Hub[SagaLogRow]]
          stream = ZStream.fromHub(hub)
            .map { row =>
              val data = s"""{"transactionId":"${row.transactionId}", "state":"${row.state}", "timestamp":${row.timestamp}}"""
              ServerSentEvent(data)
            }
        } yield Response.fromServerSentEvents(stream)
      }
    ).toHttpApp

  def serve: ZIO[Engine with Hub[SagaLogRow] with Server, Throwable, Unit] =
    Server.serve(routes)
}
