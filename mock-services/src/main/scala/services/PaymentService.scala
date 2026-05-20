package services

import zio._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde.Serde
import zio.json._
import events._

object PaymentService {

  val commandSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[ServiceCommand]).mapError(e => new RuntimeException(e))
  )(cmd => ZIO.succeed(cmd.toJson))

  val eventSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[TransactionEvent]).mapError(e => new RuntimeException(e))
  )(evt => ZIO.succeed(evt.toJson))

  def run: ZIO[Consumer with Producer, Throwable, Unit] =
    Consumer.plainStream(Subscription.topics("payment-commands"), Serde.string, commandSerde)
      .mapZIO { record =>
        record.value match {
          case cmd: ServiceCommand.ProcessPayment =>
            for {
              _ <- ZIO.logInfo(s"[PaymentService] Processing payment ${cmd.transactionId} for $$${cmd.amount}")
              _ <- ZIO.sleep(1.second) // Simulate delay
              isSuccess <- Random.nextBoolean
              // Weighted success (80%)
              isSuccessWeighted <- if (isSuccess) ZIO.succeed(true) else Random.nextBoolean
              event: TransactionEvent = if (isSuccessWeighted) 
                 TransactionEvent.PaymentProcessed(cmd.transactionId)
              else 
                 TransactionEvent.PaymentFailed(cmd.transactionId, "Insufficient Funds")
              
              _ <- ZIO.logInfo(s"[PaymentService] Result for ${cmd.transactionId}: $event")
              producer <- ZIO.service[Producer]
              _ <- producer.produceAsync(
                "saga-events",
                cmd.transactionId,
                event,
                Serde.string,
                eventSerde
              )
            } yield record.offset

          case _ => ZIO.succeed(record.offset)
        }
      }
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .runDrain
}
