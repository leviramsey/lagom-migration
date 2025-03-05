package com.example.akka

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.CoordinatedShutdown
import akka.event.Logging
import com.typesafe.config.Config
import play.api.db.DBApi
import slick.jdbc.JdbcBackend.{ Database => SlickDatabase }
import slick.util.AsyncExecutor

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

import javax.naming.InitialContext

object SlickDbProvider {
  def buildAndBindSlickDatabases(
    dbApi: DBApi,
    actorSystem: ClassicActorSystemProvider
  )(implicit ec: ExecutionContext): Unit = {
    val classicSystem = actorSystem.classicSystem
    val log = Logging.getLogger(classicSystem, getClass)
    val config = classicSystem.settings.config

    dbApi.databases().foreach { playDb =>
      val dbName = playDb.name

      try {
        val dbConfig = config.getConfig(s"db.$dbName")
        val aeConfig = dbConfig.getConfig("async-executor")
        val jndiDbName = dbConfig.getString("jndiDbName")

        val dataSource = playDb.dataSource
        val slickDb = SlickDatabase.forDataSource(
          ds = dataSource,
          maxConnections = Option(aeConfig.getInt("maxConnections")),
          executor = asyncExecutor(aeConfig))

        val namingContext = new InitialContext()
        
        namingContext.bind(jndiDbName, slickDb)

        CoordinatedShutdown(classicSystem).addTask(
          CoordinatedShutdown.PhaseServiceUnbind,
          "unbind-slick-db-from-JNDI"
        ) { () =>
          namingContext.unbind(jndiDbName)
          Future.successful(Done)
        }

        CoordinatedShutdown(classicSystem).addTask(
          CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
          "shutdown-managed-read-side-slick"
        ) { () =>
          slickDb.shutdown.map(_ => Done)
        }
      } catch {
        case util.control.NonFatal(e) =>
          log.error(e, "Failed to build/bind Slick database")
      }
    }
  }

  def asyncExecutor(aeConfig: Config): AsyncExecutor =
    AsyncExecutor(
      name = "AsyncExecutor.default",
      minThreads = aeConfig.getInt("minConnections"),
      maxThreads = aeConfig.getInt("numThreads"),
      queueSize = aeConfig.getInt("queueSize"),
      maxConnections = aeConfig.getInt("maxConnections"))

  def resolveDatabaseViaJNDI(actorSystem: ClassicActorSystemProvider): SlickDatabase = {
    val classicSystem = actorSystem.classicSystem
    val lagomReadSideConfig = classicSystem.settings.config.getConfig("lagom.persistence.read-side.jdbc")

    new InitialContext()
      .lookup(lagomReadSideConfig.getString("slick.jndiDbName"))
      .asInstanceOf[SlickDatabase]
  }
}
