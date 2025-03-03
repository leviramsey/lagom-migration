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
import scala.concurrent.duration.DurationInt

class UserToMeetupProcessor(
    readSide: SlickReadSide,
    meetupEntityResolver: String => RecipientRef[meetup.Command]
)(implicit ec: ExecutionContext, scheduler: Scheduler) extends ReadSideProcessor[user.Event] {
  override def aggregateTags = user.Event.Tag.allTags

  override def buildHandler(): ReadSideProcessor.ReadSideHandler[user.Event] = {
    val builder = readSide.builder[user.Event]("user_to_meetup")

    builder.setEventHandler[user.AuthorizedOrganizerHandoff](authorizedOrganizerHandoff)
    builder.setEventHandler[user.MeetupOrganized](meetupOrganized)

    builder.build()
  }

  private def authorizedOrganizerHandoff(elem: EventStreamElement[user.AuthorizedOrganizerHandoff]): DBIO[Done] = {
    implicit val timeout: Timeout = 5.seconds

    val evt = elem.event

    val ask =
      meetupEntityResolver(evt.meetup).ask[Done](replyTo => meetup.DesignateNextOrganizer(evt.authorized, replyTo))

    DBIO.from(ask)
  }

  private def meetupOrganized(elem: EventStreamElement[user.MeetupOrganized]): DBIO[Done] = {
    implicit val timeout: Timeout = 5.seconds

    val user = elem.entityId
    val evt = elem.event

    val ask =
      meetupEntityResolver(evt.meetup).ask[Done](replyTo => meetup.DesignateNextOrganizer(user, replyTo))

    DBIO.from(ask)
  }
}
