package com.example.meetup.impl.meetup

import akka.Done
import com.lightbend.lagom.scaladsl.persistence.EventStreamElement
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor
import com.lightbend.lagom.scaladsl.persistence.slick.SlickReadSide
import slick.dbio.DBIO
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.ExecutionContext

import java.time.Clock
import java.time.temporal.ChronoUnit

class MeetupToPostgresProcessor(
    readSide: SlickReadSide,
    repository: MeetupPostgresRepository,
    clock: Clock
) extends ReadSideProcessor[Event] {
  override def aggregateTags = Event.Tag.allTags

  override def buildHandler(): ReadSideProcessor.ReadSideHandler[Event] = {
    val builder = readSide.builder[Event]("meetup_to_postgres")

    builder.setEventHandler[Organized](organized)
    builder.setEventHandler[Described](described)
    builder.setEventHandler[Scheduled](scheduled)
    builder.setEventHandler[NextOrganizerDesignated](nextOrganizerDesignated)
    builder.setEventHandler[AttendeeRegistered](attendeeRegistered)
    builder.setEventHandler[AttendeeUnregistered](attendeeUnregistered)

    builder.build()
  }

  private def organized(elem: EventStreamElement[Organized]): DBIO[Done] = {
    val meetup = elem.entityId
    val organizer = elem.event.organizer

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
  }

  private def described(elem: EventStreamElement[Described]): DBIO[Done] = {
    val meetup = elem.entityId
    val event = elem.event

    repository.updateSummaryIfExisting(meetup) { current =>
      if (current.headline.contains(event.headline) && current.description.contains(event.description)) None
      else Some(current.copy(headline = Some(event.headline), description = Some(event.description)))
    }
  }

  private def scheduled(elem: EventStreamElement[Scheduled]): DBIO[Done] = {
    val meetup = elem.entityId
    val event = elem.event

    repository.updateSummaryIfExisting(meetup) { current =>
      if (current.startTime == event.startTime && current.endTime == event.endTime) None
      else Some(current.copy(startTime = event.startTime, endTime = event.endTime))
    }
  }

  private def nextOrganizerDesignated(elem: EventStreamElement[NextOrganizerDesignated]): DBIO[Done] = {
    val meetup = elem.entityId
    val event = elem.event

    repository.updateSummaryIfExisting(meetup) { current =>
      if (current.nextOrganizer.contains(event.nextOrganizer)) None
      else Some(current.copy(nextOrganizer = Some(event.nextOrganizer)))
    }
  }

  private def attendeeRegistered(elem: EventStreamElement[AttendeeRegistered]): DBIO[Done] = {
    val meetup = elem.entityId
    val event = elem.event

    repository.saveAttendee(MeetupAttendee(meetup, event.registered))
  }

  private def attendeeUnregistered(elem: EventStreamElement[AttendeeUnregistered]): DBIO[Done] = {
    val meetup = elem.entityId
    val event = elem.event
    
    repository.deleteAttendee(MeetupAttendee(meetup, event.unregistered))
  }
}
