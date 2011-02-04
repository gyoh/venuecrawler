package com.netprice.venuecrawler

import io.Source
import com.mongodb.{DBCollection, Mongo, BasicDBObject}
import xml.{NodeSeq, XML, Elem}
import com.weiglewilczek.slf4s.Logging
import java.util.Date
import org.quartz.{JobExecutionException, Job, JobExecutionContext}

abstract class Crawler extends Job with Logging {
  val venues = getDBCollection("venues")

  def getDBCollection(name: String): DBCollection = {
    val mongo = new Mongo("localhost" , 27017)
    val db = mongo.getDB("venuecrawler")
    val venues = db.getCollection(name)
    val index = new BasicDBObject()
    index.put("geo","2d")
    venues.ensureIndex(index)
    venues
  }

  def execute(context: JobExecutionContext) {
    try {
      val jobDetail = context.getJobDetail();
      logger info "Executing %s at %s".format(
        jobDetail.getName, new Date);
      act
      logger info "Finished %s at %s".format(
        jobDetail.getName, new Date);
    } catch {
      case e: JobExecutionException =>
        logger error "JobExecutionException occurred."
    }
  }

  def act

  def xml(url: String) = {
    val src = Source.fromURL(url)("UTF-8")
    try {
      XML.loadString(src.getLines.mkString)
    } finally {
      src.close()
    }
  }

  def nodes(url: String, op: Elem => NodeSeq) = op(xml(url))

  def handleVenues(url: String, op: Elem => Int) = op(xml(url))
}