package com.atguigu.network_flow_analysis


import com.atguigu.network_flow_analysis.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {


  def main(args: Array[String]): Unit = {

    //实时统计每小时内的网站 PV
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val userBehaviorDS: DataStream[UserBehavior] = env.readTextFile("D:\\study\\IDEA\\user_bahavior_analysis_2020\\hot_items_analysis\\src\\main\\resources\\UserBehavior.csv")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }.assignAscendingTimestamps(_.timestamp * 1000L)

    val countPV: DataStream[(String, Int)] = userBehaviorDS.filter(_.behavior == "pv")
      .map(x => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    countPV.print()

    env.execute()

  }

}
