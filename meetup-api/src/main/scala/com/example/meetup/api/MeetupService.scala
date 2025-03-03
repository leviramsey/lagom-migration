package com.example.meetup.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{ Descriptor, Service, ServiceCall }
import com.lightbend.lagom.scaladsl.api.transport.Method

trait MeetupService extends Service {
  def nop: ServiceCall[NotUsed, NotUsed]

  override final def descriptor: Descriptor = {
    import Service._

    named("meetup")
      .withCalls(
        restCall(Method.GET, "/nop", nop)
      )
      .withAutoAcl(true)
  }
}
