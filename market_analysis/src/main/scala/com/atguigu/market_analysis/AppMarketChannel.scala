package com.atguigu.market_analysis


import java.lang
import java.sql.Timestamp
import java.util.UUID

import com.atguigu.market_analysis.bean.{MarketingUserBehavior, MarketingViewCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AppMarketChannel {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val ds: DataStream[MarketingUserBehavior] = env.addSource(new MyRichSourceFunction)
      .assignAscendingTimestamps(_.timestamp)


    val value: DataStream[MarketingViewCount] = ds.filter(_.behavior != "uninstall")
      //"channel","behavior"
      .keyBy(x => (x.channel, x.behavior))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new MyProcessFunction)

    value.print()

    env.execute()

  }

}




//ProcessWindowFunction 拿到所有数据触发计算  注意类型
class MyProcessFunction extends ProcessWindowFunction[MarketingUserBehavior,MarketingViewCount,(String,String),TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingViewCount]): Unit = {

    val startTime: String = new Timestamp(context.window.getStart).toString
    val endTime: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2

    val count: Int = elements.size

    out.collect(MarketingViewCount(startTime,endTime,channel,behavior,count.toLong))

  }
}



//自定义测试数据源
class MyRichSourceFunction extends RichSourceFunction[MarketingUserBehavior] {

  var running = true

  private val userBehaviorSeq = Seq("view","download","install","uninstall")
  private val channelSeq = Seq("AppStore","mi","huawei","browser")
  private val random: Random.type = Random


  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

    val maxValue: Long = Long.MaxValue
    var counts = 0L

    while (running && counts < maxValue){

      val id: String = UUID.randomUUID().toString
      val userBehavior = userBehaviorSeq(random.nextInt(userBehaviorSeq.size))
      val channnel = channelSeq(random.nextInt(channelSeq.size))
      val ts: Long = System.currentTimeMillis()

      sourceContext.collect(MarketingUserBehavior(id,userBehavior,channnel,ts))

      counts += 1

      Thread.sleep(50L)

    }

  }

  override def cancel(): Unit = false
}
