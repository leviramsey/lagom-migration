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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class UserToMeetupProjection(
    system: ActorSystem[_],
    meetupEntityResolver: String => RecipientRef[meetup.Command],
    database: SlickDatabase
) {
  import UserToMeetupProjection._
  import system.executionContext

  implicit val scheduler: Scheduler = system.scheduler
  implicit val sys: ActorSystem[_] = system

  val projectionName = "user_to_meetup"

  def projection(tag: String) =
    SlickProjection.atLeastOnce(
      projectionId = ProjectionId(projectionName, tag),
      EventSourcedProvider.eventsByTag[user.Event](
        system = system,
        readJournalPluginId = JdbcReadJournal.Identifier,
        tag = tag),
      databaseConfig = databaseConfig,
      handler = () => new Handler(meetupEntityResolver)
    ).withSaveOffset(20, 500.milli)

  def runProjection() =
    ShardedDaemonProcess(system).init(
      name = projectionName,
      numberOfInstances = baseTag.numShards,
      behaviorFactory = (i: Int) => ProjectionBehavior(projection(shardTag(i))),
      stopMessage = ProjectionBehavior.Stop)

  val baseTag = user.Event.Tag
  def shardTag(n: Int) = s"${baseTag.tag}$n"

  val databaseConfig = new DatabaseConfig[PostgresProfile] {
    val profile = PostgresProfile
    def db = database.asInstanceOf[profile.backend.Database]
    def config = ConfigFactory.empty
    def profileName = "slick.jdbc.PostgresProfile"
    def profileIsObject = false
  }
}

object UserToMeetupProjection {
  class Handler(
      meetupEntityResolver: String => RecipientRef[meetup.Command]
  )(implicit ec: ExecutionContext, scheduler: Scheduler) extends SlickHandler[EventEnvelope[user.Event]] {
    override def process(envelope: EventEnvelope[user.Event]): DBIO[Done] = {
      implicit val timeout: Timeout = 5.seconds

      envelope.event match {
        case user.AuthorizedOrganizerHandoff(meetupId, authorized) =>
          val ask =
            meetupEntityResolver(meetupId)
              .ask[Done](replyTo => meetup.DesignateNextOrganizer(authorized, replyTo))

          DBIO.from(ask)

        case user.MeetupOrganized(meetupId) =>
          val user = envelope.persistenceId
          val ask =
            meetupEntityResolver(meetupId)
              .ask[Done](replyTo => meetup.DesignateNextOrganizer(user, replyTo))

          DBIO.from(ask)

        case _ =>
          // not interested in this event
          DBIO.successful(Done)
      }
    }
  }
}
