package poc.svend

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.util._

/**
  * General purpose Json Serdes for Kafka: able to serialize/deserialize any class V into/from the byte array expected
  * by Kafka Streams when reading/writing to Kafka.
  *
  * Here the byte array is actually a simple JSON string
  */
class JsonSerdes[V](isKey: Boolean)(implicit reader: Reads[V], writer: Writes[V]) extends Serde[V] with NoBoilerPlate {

  val logger = LoggerFactory.getLogger(this.getClass)

  private class JsonDeserializer(isKey: Boolean) extends Deserializer[V] with NoBoilerPlate {
    override def deserialize(topic: String, data: Array[Byte]): V = {
      Option(data) match {
        case None => null.asInstanceOf[V]
        case Some(byteData) =>

          val r = for {
            jsonStr <- Try(new String(data, "UTF-8"))
            .recoverWith{case th => Failure(new RuntimeException("Could not parse input byte array: not a UTF8 string?", th)) }

            parsed <- Try(Json.fromJson[V](Json.parse(jsonStr)))
              .recoverWith{case th => Failure(new RuntimeException(s"Could not parse '$jsonStr': not valid json?", th)) }

            succeded <- parsed
            match {
              case JsSuccess(value, path) => Success(value)
              // TODO: should we send them to some DLD instead?
              case JsError(errors) => Failure(new RuntimeException(s"Could not parse '$jsonStr': incorrect JSON schema?. \nError: ${errors.mkString}"))
            }
          } yield succeded

          r.get
      }

    }
  }

  private class JsonSerializer(isKey: Boolean) extends Serializer[V] with NoBoilerPlate {
    override def serialize(topic: String, data: V): Array[Byte] =
      try {
        Json.stringify(Json.toJson(data)).getBytes("UTF-8")
      } catch {
        case th =>
          logger.error(s"failed to serialize $data, giving up.")
          throw th
      }
  }

  override lazy val serializer: Serializer[V] = new JsonSerializer(isKey)

  override lazy val deserializer: Deserializer[V] = new JsonDeserializer(isKey)

}


trait NoBoilerPlate {
  def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  def close(): Unit = {}
}


