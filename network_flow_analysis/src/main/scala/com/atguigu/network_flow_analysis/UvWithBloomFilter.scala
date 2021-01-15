package com.atguigu.network_flow_analysis

import com.atguigu.network_flow_analysis.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

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
      .map(x => ("uv", x.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new MyProcessWindowFunction)


    env.execute()

  }
}


//实现自定义窗口处理函数
class MyProcessWindowFunction extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow] {


  lazy val jedis = new Jedis("192.168.30.131")
  lazy val bloomFilter = new Bloom(1<<29)


  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {


    //定义redis中存储位图的key
    val storedBitMapKey = context.window.getEnd.toString


  }

}



//定义布隆过滤器
class Bloom(size:Int) extends Serializable{

  private val cap = size

  def hash(value:String,seed:Int): Unit ={

    var result = 0

    for (i <- 0 until value.size){

      //最简单hash算法，每一位字符的ascii码值 *seed后做累加
      result = result * seed + value.charAt(i)

    }

    (cap - 1) & result
  }
}



//触发器，每来一条数据窗口计算并清空窗口状态
class MyTrigger extends Trigger[(String,Long),TimeWindow] {

  //计算和清空
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}