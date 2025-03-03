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

  log.info("Registering member-up callback")

  Cluster(system).registerOnMemberUp {
    import java.util.concurrent.ThreadLocalRandom
    import akka.pattern.after

    // Simulate some activity...
    implicit val timeout: Timeout = 5.seconds

    log.info("Simulating some traffic")

    val users = Map(
      1 -> "Albert",
      2 -> "Betty",
      3 -> "Christine",
      4 -> "Dave",
      5 -> "Edgar",
      6 -> "Frances",
      7 -> "Geraldine",
      8 -> "Hugh",
      9 -> "Ira",
      10 -> "Justine",
      11 -> "Kimberly",
      12 -> "Lex",
      13 -> "Mike",
      14 -> "Nicole",
      15 -> "Oona",
      16 -> "Peter")

    val meetups = Map(
      // (headline, description)
      1 -> ("Crocheting small chickens", "Learn to crochet small chickens!"),
      2 -> ("Start your small business", "Not a grift, I promise!"),
      3 -> ("Sparkling water appreciation", "Sample the latest flavors!"),
      4 -> ("Blub meetup", "What it says on the tin"),
      5 -> ("New parents meetup", "Babies welcome"),
      6 -> ("New parents meetup", "Just grown-ups!"),
      7 -> ("Hiking group", "Climb Mt. Watatisett"),
      8 -> ("Parents-to-be get-together", "What changes lie ahead?"),
      9 -> ("Pazz and Jop music appreciation", "Only for discerning fans"),
      10 -> ("Reactive architecture", "Responsiveness through elasticity and resilience!"),
      11 -> ("Swedish appreciation", "Sverige är jättebra!"),
      12 -> ("Model railroading", "Talk about layouts!"),
      13 -> ("Trashy romance novel appreciation", "Honour's Splendour"),
      14 -> ("Geopolitical pontifications", "No broader experience needed"),
      15 -> ("Cryptocurrency", "Talk about the latest developments"),
      16 -> ("Trekkies", "Kirk or Picard?"),
      17 -> ("Hot sauce appreciation", "Who can tolerate the most scovilles?"),
      18 -> ("Thinkpad appreciation", "It's all about the TrackPoint"),
      19 -> ("'Catcher In The Rye' is overrated", "The opposite of the Salinger Appreciation Society"),
      20 -> ("iOS development", "Talk with other app developers"),
      21 -> ("Linux users", "We're not dirty GNU hippies, we promise"),
      22 -> ("Data science/ML/AI", "Fun stuff!"),
      23 -> ("Option traders", "Attention all lovers of gamma"),
      24 -> ("Rationalists", "Don't say \"Roko's Basilisk\"")
    )

    val usersRegistered = users.map {
      case (n, name) =>
        n -> userEntityRef(s"user-$n").ask[Done](user.Register(name, _))
    }

    def iter(userFutures: Map[Int, Future[Done]], meetupFutures: Int): Future[Done] = {
      if (userFutures.isEmpty) Future.successful(Done)
      else {
        val rand = ThreadLocalRandom.current

        val probeFrom = 1 + rand.nextInt(16)

        val found = (0 to 15).foldLeft(probeFrom) { (acc, _) =>
          if (userFutures.contains(acc)) acc
          else {
            val next = (acc + 1) % (users.size + 1)
            if (next == 0) 1 else next
          }
        }

        val meetupsToOrganize =
          if (userFutures.size == 1) meetupFutures
          else {
            val ratio = meetupFutures / userFutures.size
            (rand.nextInt(2) + rand.nextInt(ratio + 1)).min(meetupFutures)
          }

        val uf = userFutures(found)
        uf.recoverWith { ex =>
          log.warning("Failed to register user [{}] {}", found, ex)
          uf
        }

        val organized =
          uf.flatMap { _ =>
            if (meetupsToOrganize == 0) Future.successful(Seq.empty[String])
            else {
              val ms = (0 until meetupsToOrganize).map { i => meetupFutures - i }
              Future.sequence(
                ms.map { i =>
                  val uid = s"user-$found"

                  log.info("User [{}] is organizing meetup [{}]", found, i)

                  val rand = ThreadLocalRandom.current

                  val startTime =
                    java.time.Instant.now().plusSeconds(rand.nextInt(172800))
                      .truncatedTo(java.time.temporal.ChronoUnit.MINUTES)

                  val endTime =
                    startTime.plus(30 + rand.nextInt(270), java.time.temporal.ChronoUnit.MINUTES)

                  userEntityRef(uid).ask[String](user.OrganizeMeetup(s"meetup-$i", _))
                    .flatMap { organized =>
                      meetupEntityRef(s"meetup-$i").ask[Done](
                        meetup.OrganizeMeetup(
                          headline = meetups(i)._1,
                          description = meetups(i)._2,
                          organizer = uid,
                          startTime = startTime,
                          endTime = endTime,
                          _))
                        .map { _ => organized }
                        .recover { ex => 
                          log.warning("Failed to organize meetup [{}] (user [{}]) {}", i, found, ex)
                          "PartiallyOrganized"
                        }
                    }
                })
            }
          }.flatMap { results =>
            if (results.forall(_ == "Organized")) Future.successful(Done)
            else Future.failed(new RuntimeException("Not all meetups could be organized"))
          }

          uf.flatMap(_ => iter(userFutures - found, meetupFutures - meetupsToOrganize))
            .flatMap(_ => organized)
      }
    }

    val allMeetupsOrganized = iter(usersRegistered, meetups.size)
    allMeetupsOrganized.onComplete {
      case Success(_) => ()

      case Failure(ex) =>
        log.error("Not all meetups registered: {}", ex)
        after(30.seconds) { system.terminate() }(system)
    }

    def userProgression(uid: String, iters: Int): Future[Done] =
      if (iters < 1) Future.successful(Done)
      else {
        val rand = ThreadLocalRandom.current
        val delay = (1 + rand.nextInt(30)).millis
        val userEntity = userEntityRef(uid)

        rand.nextInt(16) match {
          case 0 => after(delay) {
            userProgression(uid, iters / 2)
          }(system)

          case 1 => after(delay) {
            userProgression(uid, iters + 1)
          }(system)

          case 2 | 3 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 =>
            val n = 1 + rand.nextInt(meetups.size)
            after(delay) {
              meetupEntityRef(s"meetup-$n").ask[meetup.RegistrationResult](
                meetup.RegisterAttendee(uid, _)
              ).flatMap(_ => userProgression(uid, iters - 1))
            }(system)

          case 4 =>
            after(delay) {
              db.run(meetupRepo.meetupsAttending(uid)).map(_.map(_.meetupId))
                .flatMap { meetups =>
                  if (meetups.isEmpty) Future.successful(Done)
                  else {
                    val rand = ThreadLocalRandom.current
                    val n = rand.nextInt(meetups.size)
                    val selected = meetups.iterator.drop(n).next()
                    
                    meetupEntityRef(selected).ask[Done](meetup.UnregisterAttendee(uid, _))
                  }
                }.flatMap(_ => userProgression(uid, iters - 1))
            }(system)

          case 5 =>
            after(delay) {
              db.run(meetupRepo.meetupsOwnedByUser(uid)).map(_.map(_.meetupId).toSet)
                .flatMap { meetups =>
                  if (meetups.isEmpty) Future.successful(Done)
                  else {
                    db.run(meetupRepo.meetupsAttending(uid)).map(_.map(_.meetupId).toSet)
                      .flatMap { attending =>
                        val notAttending = meetups -- attending

                        if (notAttending.isEmpty) Future.successful(Done)
                        else {
                          val rand = ThreadLocalRandom.current
                          val n = rand.nextInt(notAttending.size)
                          val selected = notAttending.iterator.drop(n).next()

                          meetupEntityRef(selected).ask[meetup.RegistrationResult](
                            meetup.RegisterAttendee(uid, _)
                          ).map(_ => Done)
                        }
                      }
                  }
                }.flatMap(_ => userProgression(uid, iters - 1))
            }(system)

          case 6 =>
            after(delay) {
              db.run(meetupRepo.transferrableMeetupsFrom(uid)).map(_.map(_.meetupId).toSet)
                .flatMap { canTransfer =>
                  if (canTransfer.isEmpty) Future.successful(Done)
                  else {
                    val rand = ThreadLocalRandom.current
                    val n = 1 + rand.nextInt(users.size)
                    val nextOrganizer = s"user-$n"

                    val m = rand.nextInt(canTransfer.size)
                    val selected = canTransfer.iterator.drop(m).next()

                    if (nextOrganizer == uid) Future.successful(Done)
                    else {
                      userEntity.ask[user.HandoffResponse](
                        user.HandoffMeetupTo(selected, nextOrganizer, _)
                      ).flatMap {
                        case user.HandoffResponse.Ok =>
                          meetupEntityRef(selected).ask[Done](meetup.DesignateNextOrganizer(nextOrganizer, _))

                        case user.HandoffResponse.NotOk(_) =>
                          Future.successful(Done)
                      }
                    }
                  }
                }.flatMap(_ => userProgression(uid, iters - 1))
            }(system)
        }
      }

    allMeetupsOrganized.flatMap { _ =>
      Future.sequence(
        users.keysIterator.map { n =>
          val uid = s"user-$n"
          userProgression(uid, 1000)
        }
      )
    }.onComplete {
      case result =>
        log.info("Result is {}", result)
        system.terminate()    // ideally, this will result in some unconsumed events in the projection
    }
  }
}
