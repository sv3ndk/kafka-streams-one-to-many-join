package poc.svend
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.scala.Serdes
import org.slf4j.LoggerFactory
import poc.svend.DomainModel.{CarArrivalEvent, ZoneEvent}

import scala.util.Random

/**
  * Quick and dirty generator of fake data
  */
object FakeData extends App {

  val logger = LoggerFactory.getLogger(this.getClass)

  logger.info("starting to generate traffic...")

  val rand = new Random

  def choice(among: Seq[Int]): Int = {
    val size = among.size
    among(rand.nextInt(size))
  }

  def randomCarId(): Int = choice(among = 1 to 20)
  def randomZoneId(): Int = choice(among = 1000 to 1012)
  def randomFuelLevel(): Double = rand.nextDouble() * 50
  def randomPolutionLevel(): Double = rand.nextDouble() * 200

  def randomCarEvent(): CarArrivalEvent = CarArrivalEvent(randomCarId(), randomZoneId(), randomFuelLevel())
  def randomZoneEvent(): ZoneEvent = ZoneEvent(randomZoneId(), randomPolutionLevel())

  val carEventClient = new CarEventProducer()
  val zoneEventClient = new ZoneEventProducer()

  while (true) {

    logger.info("sending a random car event...")
    carEventClient.send(randomCarEvent())
    Thread.sleep(500)

    logger.info("sending a random zone event..." )
    zoneEventClient.send(randomZoneEvent())
    Thread.sleep(500)

  }

}

class CarEventProducer {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val carEventKafkaproducer = new KafkaProducer[Int, CarArrivalEvent](props, Serdes.Integer.serializer(), DomainModel.JsonSerdes.carArrivalEventSerdes.serializer)

  def send(carEvent: CarArrivalEvent) =
    carEventKafkaproducer.send(new ProducerRecord[Int, CarArrivalEvent](
      "car-events",
      null,
      System.currentTimeMillis,
      carEvent.car_id, carEvent))

}

class ZoneEventProducer {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val zoneEventKafkaproducer = new KafkaProducer[Int, ZoneEvent](props, Serdes.Integer.serializer(), DomainModel.JsonSerdes.zoneEventSerdes.serializer)

  def send(zoneEvent: ZoneEvent) =
    zoneEventKafkaproducer.send(new ProducerRecord[Int, ZoneEvent](
      "zone-events",
      null,
      System.currentTimeMillis,
      zoneEvent.zone_id, zoneEvent))

}
