package com.example.akka

import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.event.Logging
import play.api.libs.json._

import scala.reflect.ClassTag

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

trait JsonSerializable

// This is largely adapted from the Apache licensed Lagom 1.6.x source code
object JsonSerializer {
  def emptySingletonFormat[A <: JsonSerializable](singleton: A): Format[A] = new Format[A] {
    def writes(o: A) = JsObject(Seq.empty)
    def reads(json: JsValue) = JsSuccess(singleton)
  }

  def apply[T <: JsonSerializable : ClassTag: Format]: JsonSerializer[T] =
    JsonSerializer(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], implicitly[Format[T]], false)

  def apply[T <: JsonSerializable : ClassTag](format: Format[T]): JsonSerializer[T] =
    JsonSerializer(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], format, false)

  def compressed[T <: JsonSerializable : ClassTag: Format]: JsonSerializer[T] =
    apply[T].copy(shouldCompress = true)

  def compressed[T <: JsonSerializable : ClassTag](format: Format[T]): JsonSerializer[T] =
    apply(format).copy(shouldCompress = true)
}

final case class JsonSerializer[T](
  entityClass: Class[T],
  format: Format[T],
  shouldCompress: Boolean
)

class JsonSerializerRegistry(system: ExtendedActorSystem) extends Extension {
  val log = Logging.getLogger(system, getClass)

  def registerSerializer(jsonSerializer: JsonSerializer[_]): Unit = {
    if (_hasBeenQueried.get && warnOnRegistrationAfterQuery) {
      log.warning("Registering JsonSerializer after registry had been queried.  This may cause errors")
    }

    log.info("Registering JSON serializer for entity class [{}]", jsonSerializer.entityClass.getName)

    val singleSerializer = Seq(jsonSerializer)

    _serializers.accumulateAndGet(
      singleSerializer,
      (prev, single) => {
        prependSerializer(single.head, prev)
      }
    )
  }

  def registerSerializers(jsonSerializers: Seq[JsonSerializer[_]]): Unit = {
    if (_hasBeenQueried.get && warnOnRegistrationAfterQuery) {
      log.warning("Registering JsonSerializer after registry had been queried.  This may cause errors")
    }

    log.info(
      "Registering JSON serializers for entity classes [{}]",
      jsonSerializers.map(_.entityClass.getName).mkString(", "))

    // most specific first
    val preprocessed =
      jsonSerializers.foldLeft(Seq.empty[JsonSerializer[_]]) { (acc, v) =>
        prependSerializer(v, acc)
      }

    _serializers.accumulateAndGet(
      preprocessed,
      (prev, toAdd) => {
        toAdd.reverse   // so that we prepend the least specific first
          .foldLeft(prev) { (acc, v) =>
            prependSerializer(v, acc)
          }
      })
  }

  def registerMigration(className: String, migration: JsonMigration): Unit = {
    if (_hasBeenQueried.get && warnOnRegistrationAfterQuery) {
      log.warning("Registering JsonMigration after registry had been queried.  This may cause errors")
    }

    _migrations.put(className, migration)
  }

  def lookupSerializer(clazz: Class[_]): Option[JsonSerializer[_]] = {
    _hasBeenQueried.set(true)
    _serializers.get.find { serializer =>
      serializer.entityClass.isAssignableFrom(clazz)
    }
  }

  @annotation.tailrec
  final def lookupSerializer(manifest: String): Option[JsonSerializer[_]] = {
    _hasBeenQueried.set(true)
    
    val ret = _byClassName.updateAndGet { pair =>
      val (map, src) = pair

      if (src eq _serializers.get) pair   // No update needed
      else {
        val currentSerializers = _serializers.get
        val keysIterator = (map.keySet ++ currentSerializers.map(_.entityClass.getName)).iterator
        val keys =
          if (map.contains(manifest)) keysIterator
          else keysIterator ++ Iterator(manifest)

        keys.flatMap { m =>
          serializerForManifest(m, currentSerializers).map { serializer => m -> serializer }
        }.toMap -> currentSerializers
      }
    }._1.get(manifest)

    ret match {
      case Some(_) => ret
      case None =>
        if (serializerForManifest(manifest, _serializers.get).isDefined) {
          _byClassName.updateAndGet { pair =>
            val (map, src) = pair
            map -> Seq.empty
          }
          lookupSerializer(manifest)
        } else ret
    }
  }

  def lookupMigration(className: String): Option[JsonMigration] = {
    _hasBeenQueried.set(true)

    Option(_migrations.get(className))
  }

  private def prependSerializer(
    serializer: JsonSerializer[_],
    serializers: Seq[JsonSerializer[_]]
  ): Seq[JsonSerializer[_]] = {
    val entityClass = serializer.entityClass
    
    // remove any serializers registered for subtypes of entityClass
    serializer +: serializers.filterNot { s =>
      entityClass.isAssignableFrom(s.entityClass)
    }
  }

  private def serializerForManifest(manifest: String, serializers: Seq[JsonSerializer[_]]): Option[JsonSerializer[_]] = {
    val clazz = Class.forName(manifest)
    serializers.find(_.entityClass.isAssignableFrom(clazz))
  }

  private val warnOnRegistrationAfterQuery = {
    val config = system.settings.config
    
    if (config.hasPath(s"${JsonSerializerRegistry.ConfigPath}.warn-register-after-query")) {
      config.getBoolean(s"${JsonSerializerRegistry.ConfigPath}.warn-register-after-query")
    } else true
  }

  private val _serializers: AtomicReference[Seq[JsonSerializer[_]]] = new AtomicReference(Seq.empty)
  private val _byClassName: AtomicReference[(Map[String, JsonSerializer[_]], Seq[JsonSerializer[_]])] =
    new AtomicReference(Map.empty -> Seq.empty)

  private val _migrations: ConcurrentHashMap[String, JsonMigration] = new ConcurrentHashMap()

  private val _hasBeenQueried: AtomicBoolean = new AtomicBoolean(false)
}

object JsonSerializerRegistry extends ExtensionId[JsonSerializerRegistry] {
  val ConfigPath = "play-json-serializer.registry"

  def createExtension(system: ExtendedActorSystem): JsonSerializerRegistry =
    new JsonSerializerRegistry(system)
}
