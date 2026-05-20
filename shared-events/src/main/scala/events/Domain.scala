package events

import zio.json._

sealed trait SagaState
object SagaState {
  case object Started extends SagaState
  case object StepSucceeded extends SagaState
  case object Compensating extends SagaState
  case object Failed extends SagaState
  case object Completed extends SagaState

  implicit val codec: JsonCodec[SagaState] = DeriveJsonCodec.gen[SagaState]
}

sealed trait TransactionEvent
object TransactionEvent {
  case class OrderStarted(transactionId: String, amount: Double) extends TransactionEvent
  case class PaymentProcessed(transactionId: String) extends TransactionEvent
  case class PaymentFailed(transactionId: String, reason: String) extends TransactionEvent
  case class OrderCompensated(transactionId: String) extends TransactionEvent

  implicit val codec: JsonCodec[TransactionEvent] = DeriveJsonCodec.gen[TransactionEvent]
}

sealed trait ServiceCommand
object ServiceCommand {
  case class ProcessPayment(transactionId: String, amount: Double) extends ServiceCommand
  case class CompensateOrder(transactionId: String) extends ServiceCommand

  implicit val codec: JsonCodec[ServiceCommand] = DeriveJsonCodec.gen[ServiceCommand]
}
