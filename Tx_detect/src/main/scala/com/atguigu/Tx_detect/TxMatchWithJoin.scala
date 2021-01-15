package com.atguigu.Tx_detect

import java.net.URL

import com.atguigu.Tx_detect.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TxMatchWithJoin {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取order数据
    val res: URL = getClass.getResource("/OrderLog.csv")
    val orderDS: KeyedStream[OrderEvent, String] = env.readTextFile(res.getPath)
      .map {
        order =>
          val strings: Array[String] = order.split(",")
          OrderEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
      }
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)


    //读取receipt数据
    val res1: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptDS: KeyedStream[ReceiptEvent, String] = env.readTextFile(res1.getPath)
      .map {
        receipt =>
          val strings: Array[String] = receipt.split(",")
          ReceiptEvent(strings(0), strings(1), strings(2).toLong)
      }
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    val value: DataStream[(OrderEvent, ReceiptEvent)] = orderDS.intervalJoin(receiptDS)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new MyProcessJoinFunction)

    value.print()

    env.execute()

  }

}

class MyProcessJoinFunction extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {

  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    collector.collect(in1,in2)

  }
}