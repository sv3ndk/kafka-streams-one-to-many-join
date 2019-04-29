package poc.svend

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.kstream.{Printed, Transformer, TransformerSupplier, ValueTransformerWithKey, ValueTransformerWithKeySupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import poc.svend.CarEventLeftJoinZone.ZoneCarId
import poc.svend.DomainModel.{CarArrivalEvent, JoinedCarPollutionEvent, ZoneEvent}
import poc.svend.InterModel.{CarMove, CarMoveEvent}

import scala.collection.JavaConverters._

object DemoApp extends App {

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "one-to-many-join-demo")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "250")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder
  val streams: KafkaStreams = new KafkaStreams(Demo.buildTopology(builder), props)

  // ugly hack: cleaning up all states at every start to make the demo easier to restart
  streams.cleanUp()

  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(2))
  }
}


/**
  * The purpose of this demo is to join 2 streams of events, one with the current position of a car (car arrival events)
  * and one with the pollution level in various zones (zone events).
  *
  * Car arrival event: (car_id: Int, to_zone_id: Int, fuel_level: Double)
  * Zone pollution level event: (zone_id: Int, pollution_level: Double)
  *
  * The semantic of the join is that of a one-to-many table-to-table join by foreign key (the zone id in the case), as
  * commonly found in traditional DB systems.
  *
  * The result is a stream of "car pollution events", showing the pollution level to which each car is currently exposed.
  *
  * Joined result: (car_id: Int, zone_id: Int, fuel_level: Double, pollution_level: Double)
  *
  * The catch is that we want the join to be updated any time a new car event or zone event is received. This means we
  * can't implement this with the Kafka Streams built-in stream-to-table join, since those are not updated when the
  * table is updated, and we can't implement this with a ktable-to-ktable join either, since those are only possible for
  * equi-joint (in the current version of Kafka Streams, i.e. 2.2).
  *
  * To keep things simple, time semantics have not been taken into account in this demo => there are no timestamp
  * anywhere and events are processed in the order of arrival.
  *
  */
object Demo {

  def buildTopology(builder: StreamsBuilder): Topology = {

    import InterModel.JsonSerdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._
    import poc.svend.DomainModel.JsonSerdes._

    builder.addStateStore(CarEventLeftJoinZone.carArrivalEventStoreBuilder)
    builder.addStateStore(ZoneEventLeftJoinCar.zoneEventStoreBuilder)

    // note that the streams below are co-partitioned (by zoneId) before the join (i.e. before CarEventLeftJoinZone and
    // ZoneEventLeftJoinCar)

    val carJoinedToZones = builder

      // transforms car arrival events into car "leaving" and car "arriving" events. This is required to remove cars
      // from their previous location in the state store
      .stream[Int, CarArrivalEvent]("car-events")
      .groupByKey.aggregate(InterModel.noPrevMov)(carMoveHandler)
      .toStream
      .flatMapValues(carMoveToCarEvents _)
      .selectKey((carId, car) => car.zoneId)
      .through("car-move-events-partitioned-by-zone")

      // for each car-event, looks up the zone event to obtain the latest pollution level
      .transformValues(CarEventLeftJoinZone,
      CarEventLeftJoinZone.carArrivalEventStoreBuilder.name, ZoneEventLeftJoinCar.zoneEventStoreBuilder.name)
      .filter { case (zoneId, joinedEvent) => joinedEvent != null }

    val zonesJoinedToCars = builder
      .stream[Int, ZoneEvent]("zone-events")

      // for each zone-event, looks up all cars in that region and re-emit their pollution level
      .transform(ZoneEventLeftJoinCar,
      CarEventLeftJoinZone.carArrivalEventStoreBuilder.name, ZoneEventLeftJoinCar.zoneEventStoreBuilder.name)
      .filter { case (zoneId, joinedEvent) => joinedEvent != null }

    carJoinedToZones
      .merge(zonesJoinedToCars)
      .print(Printed.toSysOut())

    builder.build()
  }

  /**
    * Combines a new car event together with an optional previous move of the same car to deduct the new (from, to)
    * movement of that car
    **/
  def carMoveHandler(carId: Int, carEvent: CarArrivalEvent, preMove: CarMove): CarMove =
    CarMove(
      Some(preMove.toZone).filter(_ != InterModel.noPrevMov.toZone),
      carEvent.to_zone_id, carEvent.car_id, carEvent.fuel_level)

  /**
    * Transforms a car move event into up to 2 car events: one leaving the previous zone (if any) and one arriving to
    * the new zone
    */
  def carMoveToCarEvents(carMove: CarMove): Seq[CarMoveEvent] = {
    val leavingEvent = carMove.fromZone.map(fromZone => CarMoveEvent(carMove.carId, fromZone, false, carMove.fuelLevel))
    val arrivingEvent = CarMoveEvent(carMove.carId, carMove.toZone, true, carMove.fuelLevel)

    arrivingEvent +: leavingEvent.toSeq
  }

}

/**
  * Intermediary data models, used as intermediary steps in the pipeline
  */
object InterModel {

  /**
    * movement of a car from a zone id to a zone id
    */
  case class CarMove(fromZone: Option[Int], toZone: Int, carId: Int, fuelLevel: Double)

  // special marker used as 1rst value in the aggregate when there are no previously known position for a car
  // (a bit ugly, a better way would be to tune the serializer...)
  val noPrevMov = CarMove(None, -1, -1, -1d)

  /**
    * Car event at a zone: either leaving or arriving
    */
  case class CarMoveEvent(carId: Int, zoneId: Int, isArriving: Boolean, fuelLevel: Double)

  object JsonSerdes {
    implicit val carMoveSerdes: JsonSerdes[CarMove] = new JsonSerdes(false)(Json.reads[CarMove], Json.writes[CarMove])
    implicit val carEventSerdes: JsonSerdes[CarMoveEvent] = new JsonSerdes(false)(Json.reads[CarMoveEvent], Json.writes[CarMoveEvent])
  }

}


/**
  * Transformer responsible for joining cars moves to the zone state store.
  */
object CarEventLeftJoinZone extends ValueTransformerWithKeySupplier[Int, CarMoveEvent, JoinedCarPollutionEvent] {

  override def get(): ValueTransformerWithKey[Int, CarMoveEvent, JoinedCarPollutionEvent] = new CarEventProcessor()

  /**
    * composite key for the car arrival event state store,
    */
  case class ZoneCarId(zoneId: Int, carId: Int)

  object ZoneCarId {
    implicit val zoneCarIdJsonSerdes = new JsonSerdes(true)(Json.reads[ZoneCarId], Json.writes[ZoneCarId])
  }

  /**
    * store for all car arrival events, keyed by (zone id, car id)
    * => this will enable efficient range scans for a whole zone during the join triggered by a zone update (in
    * ZoneEventLeftJoinCar)
    **/
  val carArrivalEventStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("car-arrival-events-store"),
    ZoneCarId.zoneCarIdJsonSerdes,
    DomainModel.JsonSerdes.carArrivalEventSerdes)

  class CarEventProcessor extends ValueTransformerWithKey[Int, CarMoveEvent, JoinedCarPollutionEvent] {

    val logger = LoggerFactory.getLogger(this.getClass)

    private var processorContext: ProcessorContext = _
    lazy val carArrivalEventStore: KeyValueStore[ZoneCarId, CarArrivalEvent] = processorContext
      .getStateStore(carArrivalEventStoreBuilder.name)
      .asInstanceOf[KeyValueStore[ZoneCarId, CarArrivalEvent]]

    // read access to the state-store of the zone events
    lazy val zoneEventStore: KeyValueStore[Int, ZoneEvent] = processorContext
      .getStateStore(ZoneEventLeftJoinCar.zoneEventStoreBuilder.name)
      .asInstanceOf[KeyValueStore[Int, ZoneEvent]]

    override def init(context: ProcessorContext): Unit = {
      processorContext = context
    }

    override def transform(key: Int, carEvent: CarMoveEvent): JoinedCarPollutionEvent = {

      val maybeJoineResult = if (carEvent.isArriving) {

        // records this car in this zone
        carArrivalEventStore.put(
          ZoneCarId(carEvent.zoneId, carEvent.carId),
          CarArrivalEvent(carEvent.carId, carEvent.zoneId, carEvent.fuelLevel))

        // if we know the pollution level of that zone: emit a join result
        Option(zoneEventStore.get(carEvent.zoneId))
          .map { zoneEvent =>
            // logger.info("found a matching zone event to this car event!")
            JoinedCarPollutionEvent(carEvent.carId, carEvent.zoneId, carEvent.fuelLevel, zoneEvent.pollution_level)
          }.orElse {
          None

        }

      } else {
        // car is leaving => simply removes that car from that previous zone
        carArrivalEventStore.delete(ZoneCarId(carEvent.zoneId, carEvent.carId))

        // not emitting any joined result for "leaving" car events
        None
      }

      maybeJoineResult.orNull

    }

    override def close(): Unit = {}
  }
}


/**
  * Transformer responsible for joining zone events to all the cars currently present in that zone..
  *
  * => any time a new zone event is received, we re-emit all the joined results for all the cars known to be
  * in that zone.
  **/
object ZoneEventLeftJoinCar extends TransformerSupplier[Int, ZoneEvent, KeyValue[Int, JoinedCarPollutionEvent]] {

  override def get(): Transformer[Int, ZoneEvent, KeyValue[Int, JoinedCarPollutionEvent]] = new ZoneEventProcessor()

  val zoneEventStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("zone-events-store"),
    Serdes.Integer,
    DomainModel.JsonSerdes.zoneEventSerdes)

  class ZoneEventProcessor extends Transformer[Int, ZoneEvent, KeyValue[Int, JoinedCarPollutionEvent]] {

    val logger = LoggerFactory.getLogger(this.getClass)

    private var processorContext: ProcessorContext = _

    lazy val zoneEventStore: KeyValueStore[Int, ZoneEvent] = processorContext
      .getStateStore(zoneEventStoreBuilder.name)
      .asInstanceOf[KeyValueStore[Int, ZoneEvent]]

    // read access to the state-store of the car arrival events
    lazy val carArrivalEventStore: KeyValueStore[ZoneCarId, CarArrivalEvent] = processorContext
      .getStateStore(CarEventLeftJoinZone.carArrivalEventStoreBuilder.name)
      .asInstanceOf[KeyValueStore[ZoneCarId, CarArrivalEvent]]

    override def init(context: ProcessorContext): Unit = {
      processorContext = context
    }

    override def transform(key: Int, zoneEvent: ZoneEvent): KeyValue[Int, JoinedCarPollutionEvent] = {

      //      logger.info(s"zone event for  zone $key")

      zoneEventStore.put(zoneEvent.zone_id, zoneEvent)

      carArrivalEventStore
        .range(ZoneCarId(zoneEvent.zone_id, 0), ZoneCarId(zoneEvent.zone_id, Int.MaxValue))
        .asScala
        .foreach { kv =>

          val carEvent = kv.value
          this.processorContext.forward(carEvent.to_zone_id,
            JoinedCarPollutionEvent(carEvent.car_id, carEvent.to_zone_id, carEvent.fuel_level, zoneEvent.pollution_level)
          )
        }


      null
    }

    override def close(): Unit = {}
  }

}
