package com.netprice.venuecrawler

import org.quartz.impl.StdSchedulerFactory
import org.quartz._
import com.weiglewilczek.slf4s.Logging

object App extends Application with Logging {
  val scheduler = StdSchedulerFactory.getDefaultScheduler
  scheduler.start
  schedule(scheduler, classOf[GnaviCrawler])
  schedule(scheduler, classOf[TabelogCrawler])
  schedule(scheduler, classOf[HotpepperCrawler])

  def schedule(scheduler: Scheduler, clazz: Class[_]) {
    try {
      // Schedule crawler job
      val jobDetail = new JobDetail(
        clazz.getSimpleName + " Job", null, clazz)

      // fire every Wednesday at 3:00
//      val trigger = TriggerUtils.makeWeeklyTrigger(
//        TriggerUtils.WEDNESDAY, 3, 0)
//      trigger.setName(clazz.getSimpleName + " Trigger")
      val trigger = new DateIntervalTrigger(
        clazz.getSimpleName + " Trigger", null,
        DateIntervalTrigger.IntervalUnit.WEEK, 1)

      scheduler.scheduleJob(jobDetail, trigger)
    } catch {
      case e: SchedulerException =>
        logger error "SchedulerException occurred."
    }
  }

}
