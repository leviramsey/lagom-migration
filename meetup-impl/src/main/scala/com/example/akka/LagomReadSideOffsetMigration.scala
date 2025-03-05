package com.example.akka

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl._
import slick.jdbc.JdbcBackend.{ Database => SlickDatabase }
import slick.jdbc.PostgresProfile
import slick.jdbc.ResultSetType
import slick.jdbc.ResultSetConcurrency

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import java.time.Instant

object LagomReadSideOffsetMigration {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val session = SlickSession.forDbAndProfile(db(system), PostgresProfile)

    import system.dispatcher

    system.registerOnTermination(() => session.close())

    val sourceOffsetStore = new LagomSlickOffsetStore(system)
    val destOffsetStore = new AkkaSlickOffsetStore("akka_projection_offset_store", None)

    val streamDone =
      Slick.source(sourceOffsetStore.streamingOffsets)
        .wireTap(offset => system.log.info("lagom offset: {}", offset))
        .mapConcat(lagomOffsetToAkkaOffset)
        .wireTap(offset => system.log.info("akka offset: {}", offset))
        .runWith(
          Slick.sink { offsetRow =>
            import slick.jdbc.PostgresProfile.api._

            destOffsetStore.offsets += offsetRow
          }
        )

    streamDone.onComplete { result => 
      system.log.info("Stream completed: {}", result)
      system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }

  def db(system: ClassicActorSystemProvider): SlickDatabase = {
    val classicSystem = system.classicSystem
    val config = classicSystem.settings.config

    val dbConfig = config.getConfig("db.default")
    val dbUrl = dbConfig.getString("url")
    val username = dbConfig.getString("username")
    val password = dbConfig.getString("password")
    val aeConfig = dbConfig.getConfig("async-executor")

    SlickDatabase.forURL(
      url = dbUrl,
      user = username,
      password = password,
      driver = "org.postgresql.Driver",
      executor = SlickDbProvider.asyncExecutor(aeConfig))
  }

  case class LagomOffsetRow(id: String, tag: String, sequenceOffset: Option[Long], timeUuidOffset: Option[String])

  case class AkkaOffsetRow(
    projectionName: String,
    projectionKey: String,
    offsetStr: String,
    manifest: String,
    mergeable: Boolean,
    lastUpdated: Long)

  def lagomOffsetToAkkaOffset(lagom: LagomOffsetRow): Option[AkkaOffsetRow] =
    if (lagom.sequenceOffset.isEmpty && lagom.timeUuidOffset.isEmpty) None    // No offset...
    else 
      lagom.sequenceOffset match {
        case Some(sequence) =>
          val manifest = "SEQ"
          val now = Instant.now().toEpochMilli
          val asString = sequence.toString
          
          // not mergeable because our source isn't Kafka
          Some(AkkaOffsetRow(lagom.id, lagom.tag, asString, manifest, false, now))

        case None =>
          // Time UUID
          val manifest = "TBU"
          val now = Instant.now().toEpochMilli()
          val asString = lagom.timeUuidOffset.get

          Some(AkkaOffsetRow(lagom.id, lagom.tag, asString, manifest, false, now))
      }

  class LagomSlickOffsetStore(system: ClassicActorSystemProvider) {
    import PostgresProfile.api._

    val classicSystem = system.classicSystem
    val offsetStoreConfig = classicSystem.settings.config.getConfig("lagom.persistence.read-side.jdbc.tables.offset")
    val tableName = offsetStoreConfig.getString("tableName")
    val schemaName = Option(offsetStoreConfig.getString("schemaName")).filter(_.trim.nonEmpty)

    val columnsConfig = offsetStoreConfig.getConfig("columnNames")
    val idColumnName = columnsConfig.getString("readSideId")
    val tagColumnName = columnsConfig.getString("tag")
    val sequenceOffsetColumnName = columnsConfig.getString("sequenceOffset")
    val timeUuidOffsetColumnName = columnsConfig.getString("timeUuidOffset")

    val tn = tableName

    class OffsetStore(_tag: Tag)
        extends Table[LagomOffsetRow](_tag, _schemaName = schemaName, _tableName = tableName) {
      val id = column[String](idColumnName, O.Length(255, varying = true))
      val tag = column[String](tagColumnName, O.Length(255, varying = true))
      val sequenceOffset = column[Option[Long]](sequenceOffsetColumnName)
      val timeUuidOffset = column[Option[String]](timeUuidOffsetColumnName)
      val pk = primaryKey(s"${tn}_pk", id -> tag)

      def * = (id, tag, sequenceOffset, timeUuidOffset) <> (LagomOffsetRow.tupled, LagomOffsetRow.unapply)
    }

    val offsets = TableQuery[OffsetStore]

    def streamingOffsets =
      offsets.result.withStatementParameters(
        rsType = ResultSetType.ForwardOnly,
        rsConcurrency = ResultSetConcurrency.ReadOnly,
        fetchSize = 64
      )
  }

  class AkkaSlickOffsetStore(tableName: String, schemaName: Option[String]) {
    import PostgresProfile.api._


    class OffsetStore(_tag: Tag)
        extends Table[AkkaOffsetRow](_tag, _schemaName = schemaName, _tableName = tableName) {
      val Variable255 = O.Length(255, varying = true)

      val projectionName = column[String]("projection_name", Variable255)
      val projectionKey = column[String]("projection_key", Variable255)
      val offset = column[String]("current_offset", Variable255)
      val manifest = column[String]("manifest", O.Length(4, varying = true))
      val mergeable = column[Boolean]("mergeable")
      val lastUpdated = column[Long]("last_updated")

      def * =
        (projectionName, projectionKey, offset, manifest, mergeable, lastUpdated) <>
          (AkkaOffsetRow.tupled, AkkaOffsetRow.unapply)
    }

    val offsets = TableQuery[OffsetStore]
  }
}
