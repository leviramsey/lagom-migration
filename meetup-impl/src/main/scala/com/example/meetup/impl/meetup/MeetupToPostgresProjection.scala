package com.example.meetup.impl.meetup

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

import java.time.Clock
import java.time.temporal.ChronoUnit

class MeetupToPostgresProjection(
    system: ActorSystem[_],
    repository: MeetupPostgresRepository,
    clock: Clock,
    database: SlickDatabase
) {
  import MeetupToPostgresProjection._

  implicit val sys: ActorSystem[_] = system

  val projectionName = "meetup_to_postgres"

  def projection(tag: String) =
    SlickProjection.exactlyOnce(
      projectionId = ProjectionId(projectionName, tag),
      EventSourcedProvider.eventsByTag[Event](
        system = system,
        readJournalPluginId = JdbcReadJournal.Identifier,
        tag = tag),
      databaseConfig = databaseConfig,
      handler = () => new Handler(repository, clock)
    )

  def runProjection() =
    ShardedDaemonProcess(system).init(
      name = projectionName,
      numberOfInstances = baseTag.numShards,
      behaviorFactory = (i: Int) => ProjectionBehavior(projection(shardTag(i))),
      stopMessage = ProjectionBehavior.Stop)

  val baseTag = Event.Tag
  def shardTag(n: Int) = s"${baseTag.tag}$n"

  val databaseConfig = new DatabaseConfig[PostgresProfile] {
    val profile = PostgresProfile
    def db = database.asInstanceOf[profile.backend.Database]
    def config = ConfigFactory.empty
    def profileName = "slick.jdbc.PostgresProfile"
    def profileIsObject = false
  }
}

object MeetupToPostgresProjection {
  class Handler(
      repository: MeetupPostgresRepository,
      clock: Clock
  ) extends SlickHandler[EventEnvelope[Event]] {
    import PostgresProfile.api.{ DBIO => _, _ }

    override def process(envelope: EventEnvelope[Event]): DBIO[Done] = {
      envelope.event match {
        case Organized(organizer, _) =>
          val meetup = envelope.persistenceId
          
          repository.getMeetup(meetup)
            .result.headOption
            .flatMap { maybeMeetup =>
              maybeMeetup match {
                case None =>
                  repository.saveSummary(
                    MeetupSummary(
                      meetupId = meetup,
                      organizer = organizer,
                      headline = None,
                      description = None,
                      nextOrganizer = None,
                      // set a near-future start time, so events that never get scheduled will eventually get swept up
                      startTime = clock.instant().plus(24, ChronoUnit.HOURS),
                      endTime = clock.instant().plus(25, ChronoUnit.HOURS)))

                case Some(current) =>
                  if (current.organizer == organizer && current.nextOrganizer.isEmpty) DBIO.successful(Done)
                  else repository.saveSummary(current.copy(organizer = organizer, nextOrganizer = None))
              }
            }(repository.ec)

        case Described(headline, description) =>
          val meetup = envelope.persistenceId

          repository.updateSummaryIfExisting(meetup) { current =>
            if (current.headline.contains(headline) && current.description.contains(description)) None
            else Some(current.copy(headline = Some(headline), description = Some(description)))
          }

        case Scheduled(startTime, endTime) =>
          val meetup = envelope.persistenceId

          repository.updateSummaryIfExisting(meetup) { current =>
            if (current.startTime == startTime && current.endTime == endTime) None
            else Some(current.copy(startTime = startTime, endTime = endTime))
          }

        case NextOrganizerDesignated(nextOrganizer) =>
          val meetup = envelope.persistenceId

          repository.updateSummaryIfExisting(meetup) { current =>
            if (current.nextOrganizer.contains(nextOrganizer)) None
            else Some(current.copy(nextOrganizer = Some(nextOrganizer)))
          }

        case AttendeeRegistered(registered) =>
          val meetup = envelope.persistenceId

          repository.saveAttendee(MeetupAttendee(meetup, registered))

        case AttendeeUnregistered(unregistered) =>
          val meetup = envelope.persistenceId

          repository.deleteAttendee(MeetupAttendee(meetup, unregistered))
      }
    }
  }
}
