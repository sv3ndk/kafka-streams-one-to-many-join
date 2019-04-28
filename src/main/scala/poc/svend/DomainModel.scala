package poc.svend

import play.api.libs.json.Json

object DomainModel {

  // input data types
  case class CarArrivalEvent(car_id: Int, to_zone_id: Int, fuel_level: Double)
  case class ZoneEvent(zone_id: Int, pollution_level: Double)

  // output: result of the join
  case class JoinedCarPollutionEvent(car_id: Int, zone_id: Int, fuel_level: Double, pollution_level: Double)

  object JsonSerdes {
    implicit val carArrivalEventSerdes = new JsonSerdes(false)(Json.reads[CarArrivalEvent], Json.writes[CarArrivalEvent])
    implicit val zoneEventSerdes = new JsonSerdes(false)(Json.reads[ZoneEvent], Json.writes[ZoneEvent])
    implicit val carZoneEventSerdes = new JsonSerdes(false)(Json.reads[JoinedCarPollutionEvent], Json.writes[JoinedCarPollutionEvent])
  }

}

