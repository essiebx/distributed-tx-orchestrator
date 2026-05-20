package orchestrator

import zio._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.http.Server

object Main extends ZIOAppDefault {

  val producerSettings = ProducerSettings(List("localhost:19092"))
  val consumerSettings = ConsumerSettings(List("localhost:19092")).withGroupId("core-orchestrator-group")

  val producerLayer = ZLayer.scoped(Producer.make(producerSettings))
  val consumerLayer = ZLayer.scoped(Consumer.make(consumerSettings))
  val hubLayer = ZLayer.scoped(Hub.sliding[SagaLogRow](256))

  val appLayer = 
    ZLayer.make[EngineLive with Hub[SagaLogRow] with Consumer with Server](
      producerLayer,
      consumerLayer,
      hubLayer,
      SagaRepositoryLive.layer,
      io.getquill.jdbczio.Quill.Postgres.fromNamingStrategy(io.getquill.SnakeCase),
      io.getquill.jdbczio.Quill.DataSource.fromPrefix("db"),
      EngineLive.layer,
      Server.default
    )

  def run = {
    val program = for {
      _ <- ZIO.logInfo("Starting Distributed Transaction Orchestrator...")
      _ <- EngineLive.consumerStream.fork
      _ <- ApiServer.serve
    } yield ()

    program.provideLayer(appLayer)
  }
}
