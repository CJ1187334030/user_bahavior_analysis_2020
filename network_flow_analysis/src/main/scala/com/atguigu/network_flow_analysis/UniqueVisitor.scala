package com.atguigu.network_flow_analysis

import com.atguigu.network_flow_analysis.bean._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object UniqueVisitor {

  def main(args: Array[String]): Unit = {

    //一段时间（比如一小时）内访问网站的总人数
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val userBehaviorDS: DataStream[UserBehavior] = env.readTextFile("E:\\study\\IDEA\\user_bahavior_analysis_2020\\network_flow_analysis\\src\\main\\resources\\UserBehaviorTest.csv")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }.assignAscendingTimestamps(_.timestamp * 1000L)

    val value: DataStream[UvCount] = userBehaviorDS.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new MyAllWindowFunction)

    value.print()

    env.execute()

  }

}


class MyAllWindowFunction extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    //set去重
    var set: Set[Long] = Set[Long]()

    for (elem <- input)
      set += elem.userId

    out.collect(UvCount(window.getEnd,set.size))


  }
}
