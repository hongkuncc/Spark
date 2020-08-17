package com.hongkuncc.util

import java.text.SimpleDateFormat
import java.util.Calendar

object Constant {
  /*常量类*/

  /*常用时间格式*/
  val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  val dateFormat3 = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
  val dateFormat4 = new SimpleDateFormat("yyyy-MM-dd")
  val dateFormat5 = new SimpleDateFormat("yyyy/MM/dd")
  val dateFormat6 = new SimpleDateFormat("yyyyMMdd")
  val dateFormat7 = new SimpleDateFormat("yyyy-MM")
  val dateFormat8 = new SimpleDateFormat("yyyy/MM")
  val dateFormat9 = new SimpleDateFormat("yyyyMM")

  /*
  * Calendar常用类型
  * 小时、天、月、年
  * */
  val Calendar_hour = Calendar.HOUR
  val Calendar_date = Calendar.DATE
  val Calendar_month = Calendar.MONTH
  val Calendar_year = Calendar.YEAR

  /*
  * 数据库连接类型
  * */
  val PG = "org.postgresql.Driver"

  /*
  *数据追加模式append、overwrite
  *
  * */
  val Mode1 = "append"
  val Mode2 = "overwrite"

  /*
  * DataFrame默认参数
  * */
  val default_partition = 200
}
