package com.atguigu.Tx_detect

import java.net.URL

import com.atguigu.Tx_detect.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TxMatch {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取order数据
    val res: URL = getClass.getResource("/OrderLog.csv")
    val orderDs: KeyedStream[OrderEvent, String] = env.readTextFile(res.getPath)
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
    val receiptDs: KeyedStream[ReceiptEvent, String] = env.readTextFile(res1.getPath)
      .map {
        receipt =>
          val strings: Array[String] = receipt.split(",")
          ReceiptEvent(strings(0), strings(1), strings(2).toLong)
      }
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    //合并   之前已经 按照key聚合。 不用KeyedCoProcessFunction
    val value: DataStream[(OrderEvent, ReceiptEvent)] = orderDs.connect(receiptDs)
      .process(new MyCoProcessFunction)


    orderDs.print("full")


    orderDs.getSideOutput(new OutputTag[OrderEvent]("no-pay")).print("OrderEvent")
    orderDs.getSideOutput(new OutputTag[ReceiptEvent]("no-receipt")).print("ReceiptEvent")

    env.execute()

  }

}


class MyCoProcessFunction extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {

  //定义状态
  lazy val payState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay",classOf[OrderEvent]))
  lazy val receiptState:ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt",classOf[ReceiptEvent]))

  //定义侧输出流
  private val orderOutputTag = new OutputTag[OrderEvent]("no-pay")
  private val receiptOutputTag = new OutputTag[ReceiptEvent]("no-receipt")


  override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    //订单支付来了，判断之前是否有到账事件
    val receiptEvent: ReceiptEvent = receiptState.value()
    if (receiptEvent != null){
      collector.collect((in1,receiptEvent))
      payState.clear()
      receiptState.clear()
    } else{
      context.timerService().registerEventTimeTimer(in1.eventTime * 1000L + 5000L)
      payState.update(in1)

    }
  }

  override def processElement2(in2: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    //receipt支付来了，判断之前是否有pay事件
    val orderEvent: OrderEvent = payState.value()
    if (orderEvent != null){
      collector.collect((orderEvent,in2))
      payState.clear()
      receiptState.clear()
    } else{
      context.timerService().registerEventTimeTimer(in2.eventTime * 1000L + 3000L)
      receiptState.update(in2)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    if (payState.value() != null){
      ctx.output(orderOutputTag,payState.value())
    }
    if (receiptState.value() != null){
      ctx.output(receiptOutputTag,receiptState.value())
    }

    payState.clear()
    receiptState.clear()

  }
}
