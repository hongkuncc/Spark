package com.hongkuncc.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import Constant._

object DateUtil {
  def NowDate(dateformat:SimpleDateFormat): String ={
    val now = new Date()
    val date = dateformat.format(now)
    return date
  }

  def cal_time(my_format:SimpleDateFormat,flag:Int,calendar_type: String): String ={
    var types = Calendar.DATE//默认是天
    calendar_type.toLowerCase()match{
      case "hour" =>types=Calendar_hour
      case "month" =>types=Calendar_month
      case "year" =>types=Calendar_year
      case _=>types
    }
    val now_time = my_format.format(new Date())
    var dnow = my_format.parse(now_time)
    var cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(types,flag)

  }

  def getMonth(date:Date,nday:Int): String ={
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar_date,nday)
    var month = calendar.get(Calendar_month)+1
    if(month>10){
      "0"+month
    }else(
      month+""
    )
  }

}
