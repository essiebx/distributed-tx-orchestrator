package services

import zio._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde.Serde
import zio.json._
import events._

object OrderService {

  val commandSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[ServiceCommand]).mapError(e => new RuntimeException(e))
  )(cmd => ZIO.succeed(cmd.toJson))

  val eventSerde = Serde.string.inmapM((s: String) =>
    ZIO.fromEither(s.fromJson[TransactionEvent]).mapError(e => new RuntimeException(e))
  )(evt => ZIO.succeed(evt.toJson))

  def run: ZIO[Consumer with Producer, Throwable, Unit] =
    Consumer.plainStream(Subscription.topics("order-commands"), Serde.string, commandSerde)
      .mapZIO { record =>
        record.value match {
          case cmd: ServiceCommand.CompensateOrder =>
            for {
              _ <- ZIO.logInfo(s"[OrderService] Compensating order for ${cmd.transactionId}")
              _ <- ZIO.sleep(1.second) // Simulate delay
              producer <- ZIO.service[Producer]
              _ <- producer.produceAsync(
                "saga-events",
                cmd.transactionId,
                TransactionEvent.OrderCompensated(cmd.transactionId),
                Serde.string,
                eventSerde
              )
              _ <- ZIO.logInfo(s"[OrderService] Successfully compensated ${cmd.transactionId}")
            } yield record.offset

          case _ => ZIO.succeed(record.offset)
        }
      }
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .runDrain
}
