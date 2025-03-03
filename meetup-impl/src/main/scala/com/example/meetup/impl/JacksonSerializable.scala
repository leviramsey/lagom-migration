package com.example.meetup.impl

/** Marker trait for things that will be serialized via Akka's Jackson support
 *  (mainly things with ActorRefs, since those are special)
 */
trait JacksonSerializable
