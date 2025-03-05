package com.example.meetup.impl

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.RecipientRef
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.slick.SlickHandler
import akka.projection.slick.SlickProjection
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcBackend.{ Database => SlickDatabase }
import slick.jdbc.PostgresProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class MeetupToUserProjection(
    system: ActorSystem[_],
    userEntityResolver: String => RecipientRef[user.Command],
    database: SlickDatabase
) {
  import MeetupToUserProjection._
  import system.executionContext

  implicit val scheduler: Scheduler = system.scheduler
  implicit val sys: ActorSystem[_] = system

  val projectionName = "meetup_to_user"

  def projection(tag: String) =
    SlickProjection.atLeastOnce(
      projectionId = ProjectionId(projectionName, tag),
      EventSourcedProvider.eventsByTag[meetup.Event](
        system = system,
        readJournalPluginId = JdbcReadJournal.Identifier,
        tag = tag),
      databaseConfig = databaseConfig,
      handler = () => new Handler(userEntityResolver)
    ).withSaveOffset(20, 500.milli)

  def runProjection() =
    ShardedDaemonProcess(system).init(
      name = projectionName,
      numberOfInstances = baseTag.numShards,
      behaviorFactory = (i: Int) => ProjectionBehavior(projection(shardTag(i))),
      stopMessage = ProjectionBehavior.Stop)

  val baseTag = meetup.Event.Tag
  def shardTag(n: Int) = s"${baseTag.tag}$n"

  val databaseConfig = new DatabaseConfig[PostgresProfile] {
    val profile = PostgresProfile
    def db = database.asInstanceOf[profile.backend.Database]
    def config = ConfigFactory.empty
    def profileName = "slick.jdbc.PostgresProfile"
    def profileIsObject = false
  }
}

object MeetupToUserProjection {
  class Handler(
      userEntityResolver: String => RecipientRef[user.Command]
  )(implicit ec: ExecutionContext, scheduler: Scheduler) extends SlickHandler[EventEnvelope[meetup.Event]] {
    override def process(envelope: EventEnvelope[meetup.Event]): DBIO[Done] = {
      implicit val timeout: Timeout = 5.seconds

      envelope.event match {
        case meetup.Organized(organizer, previousOrganizer) =>
          val meetupId = envelope.persistenceId
          val handoff =
            if (previousOrganizer.isEmpty) Future.successful(Done)
            else userEntityResolver(previousOrganizer).ask[user.HandoffResponse] { replyTo =>
              user.RecordHandoff(meetupId, replyTo)
            }.map { _ => Done }
          
          DBIO.from(handoff)

        case meetup.AttendeeRegistered(registered) =>
          val meetupId = envelope.persistenceId

          DBIO.from(
            userEntityResolver(registered).ask[Done](replyTo => user.RecordRegistration(meetupId, replyTo)))

        case meetup.AttendeeUnregistered(attendee) =>
          val meetupId = envelope.persistenceId

          DBIO.from(
            userEntityResolver(attendee).ask[Done](replyTo => user.RecordUnregistration(meetupId, replyTo)))

        case _ =>
          // Not interesting
          DBIO.successful(Done)
      }
    }
  }
}
