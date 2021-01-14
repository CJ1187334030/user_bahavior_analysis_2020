package com.atguigu.hot_items_analysis

import java.sql.Timestamp

import com.atguigu.hot_items_analysis.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object HotItemsAnalysis {

  def main(args: Array[String]): Unit = {

    //每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //543462,1715,1464116,pv,1511658000      userId: Long,itemId: Long,categoryId: Int,behavior: String,timestamp: Long    点击、购买、收藏、喜欢
    val userBehaviorDS: DataStream[UserBehavior] = env.readTextFile("D:\\study\\IDEA\\user_bahavior_analysis_2020\\hot_items_analysis\\src\\main\\resources\\UserBehavior.csv")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }.assignAscendingTimestamps(_.timestamp * 1000L)


    val value: DataStream[String] = userBehaviorDS.filter(_.behavior == "pv")
      //使用"itemId"  返回tuple  而不是Long，后面 WindowFunction用
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new MyAggregateFunction, new MyWindowFunction)
      .keyBy("windowEnd")
      .process(new MyKeyedProcessFunction(3))

    value.print()

    env.execute()


  }


  class MyKeyedProcessFunction(takeTop:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

    lazy private val listStats:ListState[ItemViewCount] =
      getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount]))

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

      listStats.add(i)

      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val itemViewCounts = new ListBuffer[ItemViewCount]

      import scala.collection.JavaConversions._
      for (i <- listStats.get){
        itemViewCounts += i
      }

      listStats.clear()

      val listBuffer: ListBuffer[ItemViewCount] = itemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(takeTop)

      val result = new StringBuilder

      result.append("============================\n")
      result.append("时间").append(new Timestamp(timestamp -1)).append("\n")

      for (elem <- listBuffer.indices) {

        val current = listBuffer(elem)

        result.append("NO: ").append(elem+1).append(":")
          .append("item：").append(current.itemId)
          .append("view: ").append(current.count).append("\n")

      }

        result.append("===========================================\n")

        Thread.sleep(1000)

        out.collect(result.toString)

    }

  }


  class MyWindowFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count: Long = input.iterator.next()

      out.collect(ItemViewCount(itemId, window.getEnd, count))

    }
  }


  class MyAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }


}



