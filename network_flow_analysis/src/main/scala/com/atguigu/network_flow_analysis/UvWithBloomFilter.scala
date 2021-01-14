package com.atguigu.network_flow_analysis

import com.atguigu.network_flow_analysis.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object UvWithBloomFilter {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val userBehaviorDS: DataStream[UserBehavior] = env.readTextFile("E:\\study\\IDEA\\user_bahavior_analysis_2020\\network_flow_analysis\\src\\main\\resources\\UserBehaviorTest.csv")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }.assignAscendingTimestamps(_.timestamp * 1000L)


    userBehaviorDS.filter(_.behavior == "pv")
      .map(x => ("uv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
        .process()


    env.execute()

  }
}




class MyTrigger extends Trigger[(String,Long),TimeWindow] {

  //计算和清空
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}