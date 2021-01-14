package com.atguigu.network_flow_analysis

import com.atguigu.network_flow_analysis.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object UvWithBloomFilter {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val userBehaviorDS: DataStream[UserBehavior] = env.readTextFile("E:\\study\\IDEA\\user_bahavior_analysis_2020\\network_flow_analysis\\src\\main\\resources\\UserBehaviorTest.csv")
    .map {
      log =>
        val strings: Array[String] = log.split(",")
        UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
    }.assignAscendingTimestamps(_.timestamp * 1000L)



  env.execute()

}
