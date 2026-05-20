package services

import zio._
import zio.kafka.consumer._
import zio.kafka.producer._

object Main extends ZIOAppDefault {

  val producerSettings = ProducerSettings(List("localhost:19092"))
  def consumerSettings(groupId: String) = ConsumerSettings(List("localhost:19092")).withGroupId(groupId)

  val producerLayer = ZLayer.scoped(Producer.make(producerSettings))
  
  val paymentConsumerLayer = ZLayer.scoped(Consumer.make(consumerSettings("payment-service-group")))
  val orderConsumerLayer = ZLayer.scoped(Consumer.make(consumerSettings("order-service-group")))

  def run =
    for {
      _ <- ZIO.logInfo("Starting Mock Microservices (Order & Payment)...")
      f1 <- PaymentService.run.provideLayer(paymentConsumerLayer ++ producerLayer).fork
      f2 <- OrderService.run.provideLayer(orderConsumerLayer ++ producerLayer).fork
      _ <- f1.join.zipPar(f2.join)
    } yield ()
}
