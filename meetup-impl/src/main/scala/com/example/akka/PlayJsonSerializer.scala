package com.example.akka

import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.serialization.SerializerWithStringManifest
import play.api.libs.json._

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

// Replacement for and heavily inspired by the Lagom PlayJsonSerializer (Apache licensed)
class PlayJsonSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {
  import Compression._
  import PlayJsonSerializer._

  def identifier: Int = Identifier

  val log = Logging.getLogger(system, getClass)
  val conf =
    system.settings.config.getConfig("play-json-serializer")
      .withFallback(system.settings.config.getConfig("lagom.serialization.json"))

  val compressLargerThan = conf.getBytes("compress-larger-than")

  val isDebugEnabled = log.isDebugEnabled
  val registry = JsonSerializerRegistry(system)

  def serializerFor(manifest: String): Option[JsonSerializer[_]] =
    registry.lookupSerializer(manifest)

  override def manifest(o: AnyRef): String = {
    val className = o.getClass.getName
    
    registry.lookupMigration(className) match {
      case None => className
      case Some(migration) => s"$className#${migration.currentVersion}"
    }
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    val startTime = if (isDebugEnabled) System.nanoTime() else 0L
    val className = o.getClass.getName
    val serializer = serializerFor(className).getOrElse {
      throw new RuntimeException(s"Missing play-json serializer for [$className]")
    }

    val json = serializer.format.asInstanceOf[Format[AnyRef]].writes(o)
    val uncompressed = Json.stringify(json).getBytes(Charset)
    val ret =
      if (uncompressed.length > compressLargerThan && serializer.shouldCompress) compress(uncompressed)
      else uncompressed

    if (isDebugEnabled) {
      val durationMicros = (System.nanoTime - startTime) / 1000

      log.debug(
        "Serialization of [{}] took [{}] µs, size [{}] bytes",
        className,
        durationMicros,
        ret.length
      )
    }

    ret
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val startTime = if (isDebugEnabled) System.nanoTime() else 0L

    val (fromVersion, manifestClass) = parseManifest(manifest)

    val migratedManifest =
      registry.lookupMigration(manifestClass) match {
        case Some(migration) if fromVersion < migration.currentVersion =>
          migration.transformClassName(fromVersion, manifestClass)

        case Some(migration) if fromVersion == migration.currentVersion =>
          manifestClass

        case Some(migration) if fromVersion <= migration.supportedForwardVersion =>
          migration.transformClassName(fromVersion, manifestClass)

        case Some(migration) =>  // fromVersion > migration.supportedForwardVersion
          throw new IllegalStateException(
            s"Migration supported version ${migration.supportedForwardVersion} is behind " +
            s"version $fromVersion of deserialized type [$manifestClass]")

        case None => manifestClass
      }

    val jsonFormat = serializerFor(migratedManifest).getOrElse {
      throw new java.io.NotSerializableException()
    }.format.asInstanceOf[Format[AnyRef]]

    val uncompressed = if (isGzipped(bytes)) decompress(bytes) else bytes

    val json = Json.parse(bytes)

    val migratedJson =
      registry.lookupMigration(migratedManifest) match {
        case None => json
        case Some(migration) if fromVersion == migration.currentVersion => json

        case Some(migration) if fromVersion <= migration.supportedForwardVersion =>
          json match {
            case o: JsObject => migration.transform(fromVersion, o)
            case _ => migration.transformValue(fromVersion, json)
          }

        case Some(migration) =>
          throw new IllegalStateException(
            s"Migration supported version ${migration.supportedForwardVersion} is behind " +
            s"version $fromVersion of manifest [$migratedManifest]")
      }

    val ret = jsonFormat.reads(json) match {
      case JsSuccess(o, _) => o
      case JsError(errors) =>
        throw new java.io.NotSerializableException()
    }

    if (isDebugEnabled) {
      val durationMicros = (System.nanoTime - startTime) / 1000

      log.debug(
        "Deserialization of [{}] took [{}] µs, size [{}] bytes",
        manifestClass,
        durationMicros,
        bytes.length
      )
    }

    ret
  }

  private def parseManifest(manifest: String) = {
    val i = manifest.lastIndexOf('#')
    val fromVersion = if (i == -1) 1 else manifest.substring(i + 1).toInt
    val manifestClassName = if (i == -1) manifest else manifest.substring(0, i)

    fromVersion -> manifestClassName
  }
}

object PlayJsonSerializer {
  val Identifier = 1000004 // Same identifier as Lagom's PlayJsonSerializer
  val Charset = StandardCharsets.UTF_8
}

private[akka] object Compression {
  private final val BufferSize = 1024 * 4     // deliberately no type ascription: compile-time constant

  def compress(bytes: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(BufferSize)
    val zip = new GZIPOutputStream(bos)

    try {
      zip.write(bytes)
    } finally zip.close()

    bos.toByteArray
  }

  def decompress(bytes: Array[Byte]): Array[Byte] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](BufferSize)

    @annotation.tailrec
    def readChunk(): Unit =
      in.read(buffer) match {
        case -1 => ()
        case bytesRead =>
          out.write(buffer, 0, bytesRead)
          readChunk()
      }

    try {
      readChunk()
    } finally in.close()

    out.toByteArray
  }

  def isGzipped(bytes: Array[Byte]): Boolean =
    (bytes != null) && (bytes.length > 1) &&
    (bytes(0) == GzipLowByte) && (bytes(1) == GzipHighByte)

  val GzipLowByte = GZIPInputStream.GZIP_MAGIC.toByte
  val GzipHighByte = (GZIPInputStream.GZIP_MAGIC >> 8).toByte
}
