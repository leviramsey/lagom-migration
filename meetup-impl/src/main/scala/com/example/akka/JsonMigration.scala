package com.example.akka

import play.api.libs.json._

import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag

// Adapted from Lagom 1.6.x (Apache licensed)
abstract class JsonMigration(
  val currentVersion: Int,
  val supportedForwardVersion: Int
) {
  require(
    currentVersion <= supportedForwardVersion,
    s"The 'currentVersion' [$currentVersion] of a JsonMigration must be less-than or equal to " +
    s"the 'supportedForwardVersion' [$supportedForwardVersion]")

  def this(currentVersion: Int) = this(currentVersion, currentVersion)

  /** Override to define how to transform the structure of the JSON object */
  def transform(fromVersion: Int, json: JsObject): JsValue = json

  /** Override to define how to transform the structure of the JSON value */
  def transformValue(fromVersion: Int, json: JsValue): JsValue = json

  /** Override to transform the class name */
  def transformClassName(fromVersion: Int, className: String): String = className
}

/** Convenience factories */
object JsonMigration {
  def apply(
    currentVersion: Int,
    transformation: (Int, JsValue) => JsValue,
    classNameTransformation: (Int, String) => String = NoClassNameTransformation
  ): JsonMigration =
    apply(currentVersion, currentVersion, transformation, classNameTransformation)

  def apply(
    currentVersion: Int,
    supportedForwardVersion: Int,
    transformation: (Int, JsValue) => JsValue,
    classNameTransformation: (Int, String) => String
  ): JsonMigration =
    new JsonMigration(currentVersion, supportedForwardVersion) {
      override def transform(fromVersion: Int, json: JsObject) = transformation(fromVersion, json)
      override def transformValue(fromVersion: Int, json: JsValue) = transformation(fromVersion, json)
      override def transformClassName(fromVersion: Int, className: String): String =
        classNameTransformation(fromVersion, className)
    }

  def renamed(fromClassName: String, inVersion: Int, toClass: Class[_]): (String, JsonMigration) =
    renamed(fromClassName, inVersion, toClass.getName)

  def renamed(fromClassName: String, inVersion: Int, toClassName: String): (String, JsonMigration) =
    fromClassName -> new JsonMigration(inVersion) {
      override def transformClassName(fromVersion: Int, className: String) = toClassName
    }

  def transform[T: ClassTag](transformations: SortedMap[Int, Reads[JsObject]]): (String, JsonMigration) = {
    require(transformations.nonEmpty && !transformations.contains(Int.MaxValue))

    val currentVersion = transformations.maxBefore(Int.MaxValue).get._1 + 1
    transform(transformations, currentVersion, currentVersion)
  }

  def transform[T: ClassTag](
    transformations: SortedMap[Int, Reads[JsObject]],
    currentVersion: Int,
    supportedForwardVersion: Int
  ): (String, JsonMigration) = {
    val className = implicitly[ClassTag[T]].runtimeClass.getName
    className -> new JsonMigration(currentVersion, supportedForwardVersion) {
      override def transform(fromVersion: Int, json: JsObject): JsValue =
        transformations.iteratorFrom(fromVersion)
          .foldLeft(json) { (json, vt) =>
            val (version, transformation) = vt
            transformation.reads(json) match {
              case JsSuccess(transformed, _) => transformed

              case JsError(error) =>
                throw new IllegalArgumentException(
                  s"Failed to transform json from [$className] in old version, at step [$version]")
            }
          }
    }
  }

  val NoClassNameTransformation: (Int, String) => String = (_, x) => x
}
