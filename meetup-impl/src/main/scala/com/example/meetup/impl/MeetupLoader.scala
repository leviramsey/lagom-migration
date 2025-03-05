package com.example.meetup.impl

import com.example.akka.{ JsonSerializerRegistry => AkkaJsonSerializerRegistry }
import com.example.akka.SlickDbProvider
import com.example.meetup.api.MeetupService

import com.lightbend.lagom.scaladsl.api.ServiceLocator
import com.lightbend.lagom.scaladsl.api.ServiceLocator.NoServiceLocator
import com.lightbend.lagom.scaladsl.cluster.ClusterComponents
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import com.lightbend.lagom.scaladsl.playjson.EmptyJsonSerializerRegistry
import com.lightbend.lagom.scaladsl.server._
import com.softwaremill.macwire._
import play.api.db.HikariCPComponents
import play.api.db.DBComponents
import play.api.libs.ws.ahc.AhcWSComponents
import slick.jdbc.JdbcBackend.{ Database => SlickDatabase }

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
    with ClusterComponents
    with DBComponents
    with HikariCPComponents
    with AhcWSComponents {

  val _jsonSerializerRegistry = AkkaJsonSerializerRegistry(actorSystem)
  _jsonSerializerRegistry.registerSerializers(meetup.Meetup.Serializers)
  _jsonSerializerRegistry.registerSerializers(user.User.Serializers)

  override lazy val lagomServer: LagomServer = {
    val serviceImp = wire[MeetupServiceImpl]
    serverFor[MeetupService](serviceImp)
  }

  override lazy val jsonSerializerRegistry = EmptyJsonSerializerRegistry

  lazy val db: SlickDatabase = {
    // Ensure that JNDI bindings are made before spinning up cluster sharding/persistence/read-sides
    SlickDbProvider.buildAndBindSlickDatabases(dbApi, actorSystem)

    SlickDbProvider.resolveDatabaseViaJNDI(actorSystem)
  }
}
