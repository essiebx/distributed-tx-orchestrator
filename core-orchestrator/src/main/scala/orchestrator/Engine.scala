package orchestrator

import zio._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde.Serde
import zio.json._
import events._

trait Engine {
  def startSaga(transactionId: String, amount: Double): ZIO[Any, Throwable, Unit]
}

case class EngineLive(
    producer: Producer,
    repo: SagaRepository,
    hub: Hub[SagaLogRow],
    inFlight: Ref[Map[String, SagaState]]
) extends Engine {

  val commandSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[ServiceCommand]).mapError(e => new RuntimeException(e))
  )(cmd => ZIO.succeed(cmd.toJson))

  def publishState(txId: String, state: SagaState): ZIO[Any, Throwable, Unit] =
    for {
      _ <- inFlight.update(_.updated(txId, state))
      row = SagaLogRow(txId, state.toString, java.lang.System.currentTimeMillis())
      _ <- repo.insertLog(row).catchAll(e => ZIO.logError(s"DB Error: $e"))
      _ <- hub.publish(row)
      _ <- ZIO.logInfo(s"Saga $txId transitioned to $state")
    } yield ()

  override def startSaga(transactionId: String, amount: Double): ZIO[Any, Throwable, Unit] =
    for {
      _ <- publishState(transactionId, SagaState.Started)
      _ <- producer.produceAsync(
        "payment-commands",
        transactionId,
        ServiceCommand.ProcessPayment(transactionId, amount),
        Serde.string,
        commandSerde
      )
    } yield ()

  // Process incoming replies
  def processEvent(event: TransactionEvent): ZIO[Any, Throwable, Unit] = event match {
    case e: TransactionEvent.OrderStarted =>
      startSaga(e.transactionId, e.amount)
      
    case e: TransactionEvent.PaymentProcessed =>
      publishState(e.transactionId, SagaState.StepSucceeded) *>
      publishState(e.transactionId, SagaState.Completed)
      
    case e: TransactionEvent.PaymentFailed =>
      publishState(e.transactionId, SagaState.Failed) *>
      publishState(e.transactionId, SagaState.Compensating) *>
      producer.produceAsync(
        "order-commands",
        e.transactionId,
        ServiceCommand.CompensateOrder(e.transactionId),
        Serde.string,
        commandSerde
      ).unit

    case e: TransactionEvent.OrderCompensated =>
      publishState(e.transactionId, SagaState.Completed)
  }
}

object EngineLive {
  val layer: ZLayer[Producer with SagaRepository with Hub[SagaLogRow], Nothing, EngineLive] =
    ZLayer {
      for {
        producer <- ZIO.service[Producer]
        repo     <- ZIO.service[SagaRepository]
        hub      <- ZIO.service[Hub[SagaLogRow]]
        inFlight <- Ref.make(Map.empty[String, SagaState])
      } yield EngineLive(producer, repo, hub, inFlight)
    }

  val eventSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[TransactionEvent]).mapError(e => new RuntimeException(e))
  )(evt => ZIO.succeed(evt.toJson))

  def consumerStream: ZIO[EngineLive with Consumer, Throwable, Unit] =
    Consumer.plainStream(Subscription.topics("saga-events"), Serde.string, eventSerde)
      .mapZIO { record =>
        ZIO.serviceWithZIO[EngineLive](_.processEvent(record.value)).as(record)
      }
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .runDrain
}
