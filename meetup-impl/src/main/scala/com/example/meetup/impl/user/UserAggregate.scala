package com.example.meetup.impl.user

import com.example.akka.JsonSerializable
import com.example.akka.JsonSerializer
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
import play.api.libs.json._

import java.time.Clock
import java.time.Instant

sealed trait Event extends AggregateEvent[Event] with JsonSerializable {
  def aggregateTag = Event.Tag
}

object Event {
  val NumShards = 4
  val Tag = AggregateEventTag.sharded[Event]("User", NumShards)
}

case class NameSet(name: String) extends Event

object NameSet {
  implicit val format: Format[NameSet] = Json.format
}

case class MeetupOrganized(meetup: String) extends Event

object MeetupOrganized {
  implicit val format: Format[MeetupOrganized] = Json.format
}

case class AuthorizedOrganizerHandoff(meetup: String, authorized: String) extends Event

object AuthorizedOrganizerHandoff {
  implicit val format: Format[AuthorizedOrganizerHandoff] = Json.format
}

case class OrganizingHandedOff(handedOff: String) extends Event

object OrganizingHandedOff {
  implicit val format: Format[OrganizingHandedOff] = Json.format
}

case class RegisteredToAttend(meetupAttending: String) extends Event

object RegisteredToAttend {
  implicit val format: Format[RegisteredToAttend] = Json.format
}

case class UnregisteredFrom(meetupUnregistered: String) extends Event

object UnregisteredFrom {
  implicit val format: Format[UnregisteredFrom] = Json.format
}

case class MeetupHappened(meetupHappened: String) extends Event

object MeetupHappened {
  implicit val format: Format[MeetupHappened] = Json.format
}

sealed trait Command extends JacksonSerializable

// This can be used as a quick check "is this user registered"
case class GetName(replyTo: ActorRef[String]) extends Command

// Come through endpoints
case class Register(displayName: String, replyTo: ActorRef[Done]) extends Command
case class OrganizeMeetup(meetup: String, replyTo: ActorRef[String]) extends Command
case class HandoffMeetupTo(meetup: String, nextOrganizer: String, replyTo: ActorRef[HandoffResponse]) extends Command

// Projected from the meetup
case class RecordRegistration(meetup: String, replyTo: ActorRef[Done]) extends Command
case class RecordUnregistration(meetup: String, replyTo: ActorRef[Done]) extends Command
case class RecordHandoff(meetup: String, replyTo: ActorRef[HandoffResponse]) extends Command

// Projected from the clock
case class RecordMeetupHappened(meetup: String, replyTo: ActorRef[Done]) extends Command

sealed trait HandoffResponse extends JsonSerializable {
  def isOk: Boolean
}

object HandoffResponse {
  case object Ok extends HandoffResponse {
    def isOk: Boolean = true
  }

  case class NotOk(reason: Option[String]) extends HandoffResponse {
    def isOk: Boolean = false
  }

  implicit val format: Format[HandoffResponse] = new Format[HandoffResponse] {
    def writes(hr: HandoffResponse): JsValue =
      if (hr.isOk) JsObject(Map("ok" -> JsTrue))
      else hr match {
        case NotOk(Some(reason)) if reason.nonEmpty => JsObject(Map("ok" -> JsFalse, "reason" -> JsString(reason)))
        case _ => JsObject(Map("ok" -> JsFalse))
      }

    def reads(json: JsValue): JsResult[HandoffResponse] =
      json match {
        case JsObject(fields) =>
          fields.get("ok") match {
            case Some(JsTrue) => JsSuccess(Ok)

            case Some(JsFalse) =>
              val reason =
                fields.get("reason").collect {
                  case JsString(reason) => reason
                }

              JsSuccess(NotOk(reason))

            case _ => JsError("Couldn't determine if Ok")
          }
        case _ => JsError("Not an object")
      }
  }
}

object User {
  val TypeKey = EntityTypeKey[Command]("UserAggregate")
  val Serializers: Seq[JsonSerializer[_]] = Seq(
    JsonSerializer[NameSet],
    JsonSerializer[MeetupOrganized],
    JsonSerializer[AuthorizedOrganizerHandoff],
    JsonSerializer[OrganizingHandedOff],
    JsonSerializer[RegisteredToAttend],
    JsonSerializer[UnregisteredFrom],
    JsonSerializer[MeetupHappened],
    JsonSerializer[HandoffResponse.Ok.type](HandoffResponse.format.asInstanceOf[Format[HandoffResponse.Ok.type]]),
    JsonSerializer[HandoffResponse.NotOk](HandoffResponse.format.asInstanceOf[Format[HandoffResponse.NotOk]]),
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

  sealed trait OrganizedMeetup

  object OrganizedMeetup {
    case class Organizer(meetup: String) extends OrganizedMeetup
    case class HandingOffTo(meetup: String, nextOrganizer: String) extends OrganizedMeetup

    implicit val format: Format[OrganizedMeetup] = new Format[OrganizedMeetup] {
      def writes(o: OrganizedMeetup): JsValue =
        o match {
          case Organizer(meetup) => JsObject(Map("organizerFor" -> JsString(meetup)))
          case HandingOffTo(meetup, nextOrganizer) =>
            JsObject(Map("organizerFor" -> JsString(meetup), "handingOffTo" -> JsString(nextOrganizer)))
        }

      def reads(json: JsValue): JsResult[OrganizedMeetup] =
        json.asOpt[JsObject] match {
          case None => JsError("Not an object")
          case Some(obj) =>
            val keys = obj.keys

            if (keys("organizerFor")) {
              obj("organizerFor").asOpt[JsString] match {
                case Some(JsString(meetup)) =>
                  if (keys("handingOffTo")) {
                    obj("handingOffTo").asOpt[JsString] match {
                      case Some(JsString(nextOrganizer)) => JsSuccess(HandingOffTo(meetup, nextOrganizer))
                      case None => JsSuccess(Organizer(meetup))
                    }
                  } else JsSuccess(Organizer(meetup))

                case None => JsError("'organizerFor' not a string")
              }
            } else JsError("No 'organizerFor'")
        }
    }
  }

  case class State(
    displayName: String,
    organizedMeetups: Map[String, OrganizedMeetup],
    meetupsAttending: Set[String]
  ) extends JsonSerializable {
    def meetupsInvolvedWith: Set[String] = meetupsAttending ++ organizedMeetups.keys

    def processCommand(cmd: Command, clock: Clock): ReplyEffect[Event, State] =
      cmd match {
        case GetName(replyTo) => Effect.reply(replyTo)(displayName)

        case Register(desiredName, replyTo) =>
          if (displayName.nonEmpty || desiredName == displayName) Effect.reply(replyTo)(Done)
          else Effect.persist(NameSet(desiredName)).thenReply(replyTo) { _ => Done }

        case HandoffMeetupTo(meetup, nextOrganizer, replyTo) =>
          organizedMeetups.get(meetup) match {
            case None => Effect.reply(replyTo)(HandoffResponse.NotOk(Some("Did not organize meetup")))

            case Some(OrganizedMeetup.HandingOffTo(_, n)) if n == nextOrganizer =>
              Effect.reply(replyTo)(HandoffResponse.Ok)

            case Some(OrganizedMeetup.HandingOffTo(_, _)) =>
              Effect.reply(replyTo)(HandoffResponse.NotOk(Some("Handing off to someone else")))

            case Some(_) =>
              Effect.persist(AuthorizedOrganizerHandoff(meetup, nextOrganizer))
                .thenReply(replyTo) { _ => HandoffResponse.Ok }
          }

        case OrganizeMeetup(meetup, replyTo) =>
          if (organizedMeetups.contains(meetup)) Effect.reply(replyTo)("Organized")
          else if (meetupsInvolvedWith.size > 1000) Effect.reply(replyTo)("NotOrganized")
          else Effect.persist(MeetupOrganized(meetup)).thenReply(replyTo){ _ => "Organized" }

        case RecordRegistration(meetup, replyTo) =>
          if (meetupsAttending.contains(meetup)) Effect.reply(replyTo)(Done)
          else if (meetupsInvolvedWith.size > 1000) Effect.reply(replyTo)(Done)
          else Effect.persist(RegisteredToAttend(meetup)).thenReply(replyTo) { _ => Done }

        case RecordUnregistration(meetup, replyTo) =>
          if (meetupsAttending.contains(meetup)) {
            Effect.persist(UnregisteredFrom(meetup)).thenReply(replyTo) { _ => Done }
          } else Effect.reply(replyTo)(Done)

        case RecordHandoff(meetup, replyTo) =>
          organizedMeetups.get(meetup) match {
            case None => Effect.reply(replyTo)(HandoffResponse.Ok)

            case Some(_: OrganizedMeetup.HandingOffTo) =>
              Effect.persist(OrganizingHandedOff(meetup)).thenReply(replyTo) { _ => HandoffResponse.Ok }

            case Some(_) => Effect.reply(replyTo)(HandoffResponse.NotOk(Some("Not authorized")))
          }

        case RecordMeetupHappened(meetup, replyTo) =>
          if (meetupsInvolvedWith(meetup)) {
            Effect.persist(MeetupHappened(meetup)).thenReply(replyTo) { _ => Done }
          } else Effect.reply(replyTo)(Done)
      }

    def applyEvent(evt: Event): State =
      evt match {
        case NameSet(name) => copy(displayName = name)

        case MeetupOrganized(meetup) =>
          copy(organizedMeetups = organizedMeetups.updated(meetup, OrganizedMeetup.Organizer(meetup)))

        case AuthorizedOrganizerHandoff(meetup, authorized) =>
          copy(organizedMeetups = organizedMeetups.updated(meetup, OrganizedMeetup.HandingOffTo(meetup, authorized)))

        case OrganizingHandedOff(meetup) =>
          copy(organizedMeetups = organizedMeetups - meetup)

        case RegisteredToAttend(meetupAttending) => copy(meetupsAttending = meetupsAttending + meetupAttending)
        case UnregisteredFrom(meetupUnregistered) => copy(meetupsAttending = meetupsAttending - meetupUnregistered)

        case MeetupHappened(meetupHappened) =>
          copy(
            organizedMeetups = organizedMeetups - meetupHappened,
            meetupsAttending = meetupsAttending - meetupHappened)
      }
  }

  object State {
    val Empty = State(
      displayName = "",
      organizedMeetups = Map.empty,
      meetupsAttending = Set.empty
    )

    implicit val format: Format[State] = Json.format
  }
}
