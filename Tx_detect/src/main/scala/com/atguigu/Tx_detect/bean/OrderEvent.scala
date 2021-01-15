package com.atguigu.Tx_detect.bean

case class OrderEvent ( orderId: Long,
                        eventType: String,
                        txId: String,
                        eventTime: Long )
