package com.example.meetup.impl

import akka.Done
import akka.actor.typed.RecipientRef
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.util.Timeout
import com.lightbend.lagom.scaladsl.persistence.EventStreamElement
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor
import com.lightbend.lagom.scaladsl.persistence.slick.SlickReadSide
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class MeetupToUserProcessor(
    readSide: SlickReadSide,
    userEntityResolver: String => RecipientRef[user.Command]
)(implicit ec: ExecutionContext, scheduler: Scheduler) extends ReadSideProcessor[meetup.Event] {
  override def aggregateTags = meetup.Event.Tag.allTags

  override def buildHandler(): ReadSideProcessor.ReadSideHandler[meetup.Event] = {
    val builder = readSide.builder[meetup.Event]("meetup_to_user")

    builder.setEventHandler[meetup.Organized](organized)
    builder.setEventHandler[meetup.AttendeeRegistered](attendeeRegistered)
    builder.setEventHandler[meetup.AttendeeUnregistered](attendeeUnregistered)

    builder.build()
  }

  private def organized(elem: EventStreamElement[meetup.Organized]): DBIO[Done] = {
    implicit val timeout: Timeout = 5.seconds

    val meetup = elem.entityId
    val evt = elem.event

    val handoff =
      if (evt.previousOrganizer.isEmpty) Future.successful(Done)
      else userEntityResolver(evt.previousOrganizer).ask[user.HandoffResponse] { replyTo =>
        user.RecordHandoff(meetup, replyTo)
      }.map { _ => Done }

    DBIO.from(handoff)
  }

  private def attendeeRegistered(elem: EventStreamElement[meetup.AttendeeRegistered]): DBIO[Done] = {
    implicit val timeout: Timeout = 5.seconds

    val meetup = elem.entityId
    val evt = elem.event

    DBIO.from(
      userEntityResolver(evt.registered).ask[Done](replyTo => user.RecordRegistration(meetup, replyTo)))
  }

  private def attendeeUnregistered(elem: EventStreamElement[meetup.AttendeeUnregistered]): DBIO[Done] = {
    implicit val timeout: Timeout = 5.seconds

    val meetup = elem.entityId
    val evt = elem.event

    DBIO.from(
      userEntityResolver(evt.unregistered).ask[Done](replyTo => user.RecordUnregistration(meetup, replyTo)))
  }
}
