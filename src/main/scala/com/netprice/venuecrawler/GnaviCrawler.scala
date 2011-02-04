package com.netprice.venuecrawler

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date
import com.mongodb.BasicDBObject
import xml.Node

class GnaviCrawler extends Crawler {
  val keyid = "5c30463d884796d3c7e882d5537f70ed"
  val coordinatesMode = 2 // WGS
  val sort = 1
  val offset = 1
  val hitPerPage = 100

  def baseURL(criterion: String) =
    "http://api.gnavi.co.jp/ver1/%s/?keyid=%s"
            .format(criterion, keyid) 

  def act {
    val prefs = nodes(baseURL("PrefSearchAPI"),
      xml => xml \\ "pref_code")
    val cats = nodes(baseURL("CategorySmallSearchAPI"),
      xml => xml \\ "category_s_code")

    // This is a workaround for the gnavi API.
    // Gnavi API only allows max 1000 records for output.
    // Therefore, we divide records into smaller pieces
    // by prefectures and categories.
    for (pref <- prefs) {
      for (cat <- cats) {
        fetch(pref.text, cat.text, 1)
      }
    }
  }

  private def fetch(pref: String, cat: String, pageId: Int) {
    Thread.sleep(1000)

    val params = Map(
      "pref" -> pref,
      "category_s" -> cat,
      "coordinates_mode" -> coordinatesMode,
      "sort" -> sort,
      "offset" -> offset,
      "hit_per_page" -> hitPerPage,
      "offset_page" -> pageId)

    val url = (baseURL("RestSearchAPI") /:
            params.map(pair => pair._1 + "=" + pair._2)
            ) (_ + "&" + _)

    val total = handleVenues(url, xml =>
      (xml \\ "error" \ "code").text match {
        case "" =>
          (xml \\ "rest").foreach(store)
          (xml \ "total_hit_count").text.toInt
        case "600" => logger debug "No venues found."; 0
        case "601" => logger error "Invalid access."; 0
        case "602" => logger error "Invalid shop number."; 0
        case "603" => logger error "Invalid type."; 0
        case "604" => logger error "Internal server error."; 0
        case s: String => logger error "Error code: " + s; 0
        case _ => 0
      }
    )

    logger info "%s, %s, %s, %s".format(pref, cat, total, pageId)
    if (hitPerPage * pageId < total) fetch(pref, cat, pageId + 1)
  }

  private def store(rest: Node) {
    val url = (rest \ "url").text.split("\\?")(0)

    val criteria = new BasicDBObject()
    criteria.put("url", url)

    val venue = new BasicDBObject()
    venue.put("name", (rest \ "name").text)
    val zipAddress = splitAddress((rest \ "address").text)
    venue.put("address", zipAddress(1))
    venue.put("zip", zipAddress(0))
    venue.put("url", url)
    val geo = new BasicDBObject()
    geo.put("latitude", (rest \ "latitude").text.toDouble)
    geo.put("longitude", (rest \ "longitude").text.toDouble)
    venue.put("geo", geo)
    venue.put("lastUpdated", parseDate((rest \ "update_date").text))

    venues.update(criteria, venue, true, false)
  }

  private def parseDate(dateStr: String): Date = {
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      sdf.parse(dateStr);
    } catch {
      case e: ParseException =>
        logger error "Invalid date: %s".format(dateStr)
        new Date
    }
  }

  private def splitAddress(address: String): Array[String] = {
    val result = address.split(" ")
    Array(result(0).substring(1), result(1))
  }

}