package com.netprice.venuecrawler

import xml.Node
import com.mongodb.BasicDBObject
import java.util.Date

class HotpepperCrawler extends Crawler {
  val key = "16be4f78882293a9"
  val datum = "world" // WGS
  val kind = "lite"
  val count = 100;

  def baseURL(criterion: String) =
    "http://webservice.recruit.co.jp/hotpepper/%s/v1/?key=%s"
            .format(criterion, key)

  def act {
    val prefs = nodes(baseURL("large_area"),
      xml => xml \\ "large_area" \ "code")
    prefs.foreach((pref: Node) => fetch(pref.text, 1))
  }

  private def fetch(pref: String, start: Int) {
    Thread.sleep(1000)

    val params = Map(
      "large_area" -> pref,
      "datum" -> datum,
      "type" -> kind,
      "count" -> count,
      "start" -> start)

    val url = (baseURL("gourmet") /:
            params.map(pair => pair._1 + "=" + pair._2)
            ) (_ + "&" + _)

    val total = handleVenues(url, xml =>
      (xml \\ "error" \ "message").text match {
        case "" =>
          (xml \\ "shop").foreach(store)
          (xml \ "results_available").text.toInt
        case s: String => logger error s; 0
        case _ => 0
      }
    )

    logger info "%s, %s, %s".format(pref, total, start)
    if (start + count < total) fetch(pref, start + count)
  }

  private def store(shop: Node) {
    val url = (shop \\ "urls" \ "pc").text.split("\\?")(0)

    val criteria = new BasicDBObject()
    criteria.put("url", url)

    val venue = new BasicDBObject()
    venue.put("name", (shop \ "name").text)
    venue.put("address", (shop \ "address").text)
    venue.put("url", url)
    val geo = new BasicDBObject()
    geo.put("latitude", (shop \ "lat").text.toDouble)
    geo.put("longitude", (shop \ "lng").text.toDouble)
    venue.put("geo", geo)
    venue.put("lastUpdated", new Date)

    venues.update(criteria, venue, true, false)
  }
}