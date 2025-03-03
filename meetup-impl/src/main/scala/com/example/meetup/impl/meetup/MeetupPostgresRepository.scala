package com.example.meetup.impl.meetup

import akka.Done
import slick.jdbc.PostgresProfile.api._
import slick.sql.SqlProfile.ColumnOption.SqlType

import scala.concurrent.ExecutionContext

import java.time.Instant

class MeetupPostgresRepository(implicit val ec: ExecutionContext) {
  class MeetupSummaryTable(tag: Tag) extends Table[MeetupSummary](tag, "meetup_summary") {
    def * =
      (meetupId, organizer, headline, description, nextOrganizer, startTime, endTime) <>
        (MeetupSummary.tupled, MeetupSummary.unapply)

    def meetupId = column[String]("meetup_id", O.PrimaryKey, varchar255)
    def organizer = column[String]("organizer", varchar255)
    def headline = column[Option[String]]("headline", text)
    def description = column[Option[String]]("description", text)
    def nextOrganizer = column[Option[String]]("next_organizer", varchar255)
    def startTime = column[Instant]("start_time")
    def endTime = column[Instant]("end_time")
  }

  val meetupSummaries = TableQuery[MeetupSummaryTable]

  def getMeetup(id: String) = meetupSummaries.filter(_.meetupId === id)
  def saveSummary(summary: MeetupSummary) = meetupSummaries.insertOrUpdate(summary).map(_ => Done)

  def meetupsOwnedByUserQuery(uid: String) = meetupSummaries.filter(_.organizer === uid)
  def transferrableMeetupsFrom(uid: String) = meetupsOwnedByUserQuery(uid).filter(_.nextOrganizer.isEmpty).result
  def meetupsOwnedByUser(uid: String) = meetupSummaries.filter(_.organizer === uid).result

  def updateSummaryIfExisting(meetupId: String)(updater: MeetupSummary => Option[MeetupSummary]): DBIO[Done] =
    getMeetup(meetupId).result.headOption
      .flatMap { maybeMeetup =>
        maybeMeetup match {
          case None => DBIO.successful(Done)
          case Some(current) =>
            updater(current) match {
              case None => DBIO.successful(Done)
              case Some(toInsert) => saveSummary(toInsert)
            }
        }
      }

  class MeetupAttendeesTable(tag: Tag) extends Table[MeetupAttendee](tag, "meetup_attendees") {
    def * = (meetupId, attendee) <> (MeetupAttendee.tupled, MeetupAttendee.unapply)

    def meetupId = column[String]("meetup_id", varchar255)
    def attendee = column[String]("attendee", varchar255)

    def pk = primaryKey("pk", (meetupId, attendee))
    def attendeeIndex = index("idx_attendee", attendee)
  }

  val meetupAttendees = TableQuery[MeetupAttendeesTable]

  def specificAttendee(ma: MeetupAttendee) =
    meetupAttendees.filter(_.meetupId === ma.meetupId).filter(_.attendee === ma.attendee)

  def saveAttendee(ma: MeetupAttendee) = {
    val selectQ = specificAttendee(ma).exists.result

    selectQ.flatMap { exists =>
      if (exists) DBIO.successful(Done)
      else {
        val q = meetupAttendees += ma
    
        q.map(_ => Done)
      }
    }
  }

  def attendeesFor(meetup: String) = meetupAttendees.filter(_.meetupId === meetup)
  def meetupsAttending(attendee: String) = meetupAttendees.filter(_.attendee === attendee).result

  def deleteAttendee(ma: MeetupAttendee) = {
    val q = specificAttendee(ma).delete
    q.map(_ => Done)
  }

  val varchar255 = SqlType("VARCHAR(255)")
  val text = SqlType("TEXT")
}

case class MeetupSummary(
    meetupId: String,
    organizer: String,
    headline: Option[String],
    description: Option[String],
    nextOrganizer: Option[String],
    startTime: Instant,
    endTime: Instant)

case class MeetupAttendee(
  meetupId: String,
  attendee: String)
