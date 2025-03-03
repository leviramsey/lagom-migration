package com.example.meetup.impl

import com.example.meetup.api.MeetupService

import akka.cluster.sharding.typed.scaladsl.Entity
import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import com.lightbend.lagom.scaladsl.persistence.slick.SlickPersistenceComponents
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.lightbend.lagom.scaladsl.server._
import com.softwaremill.macwire._
import play.api.db.HikariCPComponents
import play.api.libs.ws.ahc.AhcWSComponents
import com.lightbend.lagom.scaladsl.persistence.jdbc.JdbcPersistenceComponents

class MeetupLoader extends LagomApplicationLoader {
  override def load(context: LagomApplicationContext): LagomApplication =
    new MeetupApplication(context) {
      override def serviceLocator: ServiceLocator = NoServiceLocator
    }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication =
    load(context)

  override def describeService = None
}

abstract class MeetupApplication(context: LagomApplicationContext) extends
    LagomApplication(context)
    with JdbcPersistenceComponents
    with SlickPersistenceComponents
    with HikariCPComponents
    with AhcWSComponents {

  actorSystem.log.warning("config: {}", actorSystem.settings.config.getValue("jdbc-snapshot-store"))
  override lazy val lagomServer: LagomServer = {
    val serviceImp = wire[MeetupServiceImpl]
    serverFor[MeetupService](serviceImp)
  }

  override lazy val jsonSerializerRegistry: JsonSerializerRegistry = MeetupSerializerRegistry
}
