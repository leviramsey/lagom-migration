package com.example.meetup.impl.meetup

import com.example.meetup.impl.JacksonSerializable

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.EntityContext
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import com.lightbend.lagom.scaladsl.persistence.AggregateEvent
import com.lightbend.lagom.scaladsl.persistence.AggregateEventTag
import com.lightbend.lagom.scaladsl.persistence.AkkaTaggerAdapter
import com.lightbend.lagom.scaladsl.playjson.JsonSerializer
import play.api.libs.json._

import java.time.Clock
import java.time.Instant

sealed trait Event extends AggregateEvent[Event] {
  def aggregateTag = Event.Tag
}

object Event {
  val NumShards = 8
  val Tag = AggregateEventTag.sharded[Event]("Meetup", NumShards)
}

case class Organized(organizer: String, previousOrganizer: String) extends Event

object Organized {
  implicit val format: Format[Organized] = Json.format
}

case class Described(headline: String, description: String) extends Event

object Described {
  implicit val format: Format[Described] = Json.format
}

case class Scheduled(startTime: Instant, endTime: Instant) extends Event

object Scheduled {
  implicit val format: Format[Scheduled] = Json.format
}

case class NextOrganizerDesignated(nextOrganizer: String) extends Event

object NextOrganizerDesignated {
  implicit val format: Format[NextOrganizerDesignated] = Json.format
}

case class AttendeeRegistered(registered: String) extends Event

object AttendeeRegistered {
  implicit val format: Format[AttendeeRegistered] = Json.format
}

case class AttendeeUnregistered(unregistered: String) extends Event

object AttendeeUnregistered {
  implicit val format: Format[AttendeeUnregistered] = Json.format
}

sealed trait Command extends JacksonSerializable

case class OrganizeMeetup(
  headline: String,
  description: String,
  organizer: String,
  startTime: Instant,
  endTime: Instant,
  replyTo: ActorRef[Done]
) extends Command

case class Reschedule(startTime: Instant, endTime: Instant, replyTo: ActorRef[Done]) extends Command
case class DesignateNextOrganizer(nextOrganizer: String, replyTo: ActorRef[Done]) extends Command
case class RegisterAttendee(attendee: String, replyTo: ActorRef[RegistrationResult]) extends Command
case class UnregisterAttendee(attendee: String, replyTo: ActorRef[Done]) extends Command

sealed trait RegistrationResult

object RegistrationResult {
  case object Registered extends RegistrationResult
  case object NotRegistered extends RegistrationResult

  implicit val format: Format[RegistrationResult] = new Format[RegistrationResult] {
    def writes(rr: RegistrationResult): JsValue =
      if (rr eq Registered) JsString("REGISTERED")
      else JsString("NOT_REGISTERED")

    def reads(json: JsValue): JsResult[RegistrationResult] =
      json match {
        case JsString("REGISTERED") => JsSuccess(Registered)
        case JsString(_) => JsSuccess(NotRegistered)
        case _ => JsError("Not the strings 'REGISTERED' or 'NOT_REGISTERED'")
      }
  }
}

object Meetup {
  val TypeKey = EntityTypeKey[Command]("MeetupAggregate")
  val Serializers: Seq[JsonSerializer[_]] = Seq(
    JsonSerializer[Described],
    JsonSerializer[Scheduled],
    JsonSerializer[Organized],
    JsonSerializer[AttendeeRegistered],
    JsonSerializer[AttendeeUnregistered],
    JsonSerializer[RegistrationResult.Registered.type](
      RegistrationResult.format.asInstanceOf[Format[RegistrationResult.Registered.type]]),
    JsonSerializer[RegistrationResult.NotRegistered.type](
      RegistrationResult.format.asInstanceOf[Format[RegistrationResult.NotRegistered.type]]),
    JsonSerializer[NextOrganizerDesignated],
    JsonSerializer.compressed[State]
  )

  def create(entityContext: EntityContext[Command]): Behavior[Command] = {
    val persistenceId = PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)

    create(persistenceId, Clock.systemUTC)
      .withTagger(
        AkkaTaggerAdapter.fromLagom(entityContext, Event.Tag))
  }

  /** For testing, doesn't require sharding infra */
  private[meetup] def create(persistenceId: PersistenceId, clock: Clock) =
    EventSourcedBehavior.withEnforcedReplies[Command, Event, State](
      persistenceId = persistenceId,
      emptyState = State.Empty,
      commandHandler = (meetup, cmd) => meetup.processCommand(cmd, clock),
      eventHandler = (meetup, evt) => meetup.applyEvent(evt)
    )

  case class State(
    headline: String,
    description: String,
    organizer: String,
    permittedNextOrganizer: Option[String],
    startTime: Instant,
    endTime: Instant,
    attendees: Set[String]
  ) {
    def organized: Boolean = (headline.nonEmpty || description.nonEmpty || organizer.nonEmpty)

    def processCommand(cmd: Command, clock: Clock): ReplyEffect[Event, State] =
      cmd match {
        case OrganizeMeetup(headline, description, organizer, startTime, endTime, replyTo) =>
          if (State.Empty == this)
            Effect.persist(
              Seq(Organized(organizer, ""), Described(headline, description), Scheduled(startTime, endTime))
            ).thenReply(replyTo) { _ => Done }
          else if (State.Empty.copy(organizer = organizer) == this) {
            // Possible if creating a new meetup and the UserToMeetupProcessor got here first
            Effect.persist(
              Seq(Described(headline, description), Scheduled(startTime, endTime))
            ).thenReply(replyTo) { _ => Done }
          } else Effect.reply(replyTo)(Done)

        case Reschedule(startTime, endTime, replyTo) =>
          if (organized && startTime.isAfter(clock.instant()))
            Effect.persist(Scheduled(startTime, endTime)).thenReply(replyTo) { _ => Done }
          else Effect.reply(replyTo)(Done)

        case DesignateNextOrganizer(nextOrganizer, replyTo) =>
          if (nextOrganizer == organizer) Effect.reply(replyTo)(Done)
          else if (permittedNextOrganizer.contains(nextOrganizer)) {
            // already designated, so this makes it official
            Effect.persist(Organized(nextOrganizer, organizer)).thenReply(replyTo) { _ => Done }
          } else if (permittedNextOrganizer.nonEmpty) {
            // someone else is designated
            Effect.reply(replyTo)(Done)
          } else Effect.persist(NextOrganizerDesignated(nextOrganizer)).thenReply(replyTo) { _ => Done }

        case RegisterAttendee(attendee, replyTo) =>
          if (attendees(attendee)) Effect.reply(replyTo)(RegistrationResult.Registered)
          else if (attendees.size >= 1000) Effect.reply(replyTo)(RegistrationResult.NotRegistered)
          else Effect.persist(AttendeeRegistered(attendee)).thenReply(replyTo) { state =>
            if (state.attendees(attendee)) RegistrationResult.Registered
            else RegistrationResult.NotRegistered
          }

        case UnregisterAttendee(attendee, replyTo) =>
          if (attendees(attendee)) Effect.persist(AttendeeUnregistered(attendee)).thenReply(replyTo) { _ => Done }
          else Effect.reply(replyTo)(Done)
      }

    def applyEvent(evt: Event): State =
      evt match {
        case Organized(organizer, _) => copy(organizer = organizer, permittedNextOrganizer = None)
        case Described(headline, description) => copy(headline = headline, description = description)
        case Scheduled(startTime, endTime) => copy(startTime = startTime, endTime = endTime)
        case NextOrganizerDesignated(nextOrganizer) => copy(permittedNextOrganizer = Some(nextOrganizer))
        case AttendeeRegistered(registered) => copy(attendees = attendees + registered)
        case AttendeeUnregistered(unregistered) => copy(attendees = attendees - unregistered)
      }
  }

  object State {
    val Empty = State(
      headline = "",
      description = "",
      organizer = "",
      permittedNextOrganizer = None,
      startTime = Instant.MIN,
      endTime = Instant.MAX,
      attendees = Set.empty
    )

    implicit val format: Format[State] = Json.format
  }
}
