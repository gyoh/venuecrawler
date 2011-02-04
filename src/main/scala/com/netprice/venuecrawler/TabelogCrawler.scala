package com.netprice.venuecrawler

import scala.collection.JavaConversions._
import com.mongodb.BasicDBObject
import java.util.Date
import java.net.{MalformedURLException, URL}
import net.htmlparser.jericho.{HTMLElementName, Source}

class TabelogCrawler extends Crawler {
  val host = "http://r.tabelog.com"
  val path = "/sitemap/"
  val count = 200
  var loop = 0

  def act {
    getPaths(host + path)
  }

  private def getPaths(url: String) {
    handleHtml(source(url), src => {
      val arealst = src.getElementById("arealst_sitemap")
      arealst match {
        case null =>
          loop = 0
          getVenues(url, src, 1)
        case _ =>
          loop += 1
          if (loop > 3) {
            logger error "Inifite loop detected at: %s".format(url)
            return
          }
          val elements = arealst.getAllElements(HTMLElementName.LI)
          for (element <- elements.toList) {
            val link = element.getFirstElement(HTMLElementName.A)
            if (link != null) {
              var href = link.getAttributeValue("href")
              // workaround for stupid bug in tabelog
              href = href.replaceAll("fukusima", "fukushima")
              if (!href.startsWith(host)) {
                href = host + href
              }
              getPaths(href)
            }
          }
      }
    })
  }

  private def getVenues(url: String, orgSrc: Source, pageId: Int) {
    logger info "page: %s".format(pageId)

    val src = pageId match {
      case 1 => orgSrc
      case _ => source(url + "?PG=" + pageId)
    }

    handleHtml(src, src => {
      val rstlst = src.getElementById("rstlst_sitemap")

      // Get the total count.
      val totalElement = rstlst.getFirstElementByClass("result_num")
      val total = totalElement.getFirstElement("strong")
              .getContent.toString.toInt
      logger info "total venues: %s".format(total)

      // Generate a list of venues.
      val elements = rstlst.getAllElementsByClass("rstname")
      for (element <- elements.toList) {
        val venuePath = element.getFirstElement(HTMLElementName.A)
                .getAttributeValue("href")
        store(host + venuePath)
      }

      if (count * pageId < total) getVenues(url, src, pageId + 1)
    })
  }

  private def store(url: String) {
    handleHtml(source(url), src => {
      val criteria = new BasicDBObject
      criteria.put("url", url)

      val venue = new BasicDBObject
      val rstData = src.getFirstElementByClass("rst-data")
      val rstName = rstData.getFirstElementByClass("rst-name")
      venue.put("name", rstName.getFirstElement(HTMLElementName.STRONG)
              .getContent.toString)

      // Get address chunks and concatenate them
      val values = List("v:region", "v:locality", "v:street-address")
      var address = ""
      values.foreach(value => {
        val element = rstData.getFirstElement("property", value, false)
        if (element != null) {
          address += element.getContent.getTextExtractor.toString
        }
      })
      venue.put("address", address)
      venue.put("url", url)

      // Extract latitude and longitude from the google map url parameters
      val rstMap = src.getFirstElementByClass("rst-map")
      val map = rstMap.getFirstElement(HTMLElementName.IMG)
      val mapLink = map.getAttributeValue("src")
      getParameter(mapLink, "markers") match {
        case Some(markers) =>
          val latlong = markers.split(",")
          val geo = new BasicDBObject()
          geo.put("latitude", latlong(0).toDouble)
          geo.put("longitude", latlong(1).toDouble)
          venue.put("geo", geo)
        case None =>
      }
      venue.put("lastUpdated", new Date)

      venues.update(criteria, venue, true, false)
    })
  }

  private def source(url: String) = {
    Thread.sleep(1000)
    logger info url
    val in = new URL(url).openStream
    try {
      val src = new Source(in)
      src.fullSequentialParse()
      src
    } finally {
      in.close()
    }
  }

  private def handleHtml(src: Source, op: Source => Unit) = {
    try {
      op(src)
    } catch {
      case e: Exception =>
        logger error "Exception occurred while processing html."
    }
  }

  private def getParameter(urlStr: String, key: String) = {
    try {
      val url = new URL(urlStr)
      val query = url.getQuery()
      val params = query.split("&").filter(param => {
        param.split("=")(0) == key
      })
      Some(params.head.split("=")(1))
    } catch {
      case e: MalformedURLException =>
        logger error "Invalid URL."
        None
    }
  }
}