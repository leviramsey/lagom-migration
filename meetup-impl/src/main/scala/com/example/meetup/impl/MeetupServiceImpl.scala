package com.example.meetup.impl

import com.example.meetup.api.MeetupService

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.actor.typed.{ ActorSystem => TypedActorSystem }
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.Cluster
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.util.Timeout
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.persistence.ReadSide
import com.lightbend.lagom.scaladsl.persistence.slick.SlickReadSide
import com.softwaremill.macwire._
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Success }

import java.time.Clock

class MeetupServiceImpl(
  clusterSharding: ClusterSharding,
  readSide: ReadSide,
  slickReadSide: SlickReadSide,
  system: ActorSystem,
  db: Database
) extends MeetupService {
  import system.dispatcher
  val log = system.log

  override def nop: ServiceCall[NotUsed,NotUsed] = ServiceCall { _ => Future.successful(NotUsed) }

  def transferrableMeetups(uid: String): Future[Set[meetup.MeetupSummary]] =
    db.run(meetupRepo.transferrableMeetupsFrom(uid)).map(_.toSet)

  log.info("Construcing MeetupServiceImpl")

  val typedSystem = system.toTyped
  implicit val scheduler: Scheduler = typedSystem.scheduler

  val meetupRepo = wire[meetup.MeetupPostgresRepository]

  clusterSharding.init(
    Entity(meetup.Meetup.TypeKey)(entityContext => meetup.Meetup.create(entityContext)))

  clusterSharding.init(
    Entity(user.User.TypeKey)(entityContext => user.User.create(entityContext)))


  readSide.register(new MeetupToUserProcessor(slickReadSide, userEntityRef(_)))
  readSide.register(new UserToMeetupProcessor(slickReadSide, meetupEntityRef(_)))
  readSide.register(new meetup.MeetupToPostgresProcessor(slickReadSide, meetupRepo, Clock.systemUTC()))

  private def meetupEntityRef(meetupId: String): EntityRef[meetup.Command] =
    clusterSharding.entityRefFor(meetup.Meetup.TypeKey, meetupId)

  private def userEntityRef(userId: String): EntityRef[user.Command] = {
    clusterSharding.entityRefFor(user.User.TypeKey, userId)
  }
}
