package com.atguigu.market_analysis.bean

case class MarketingViewCount (windowStart: String,
                               windowEnd: String,
                               channel: String,
                               behavior: String,
                               count: Long)
