package com.hongkuncc.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtil {
  def NowDate(dateformat:SimpleDateFormat): String ={
    val now = new Date()
    val date = dateformat.format(now)
    return date
  }
  def cal_time(my_format:SimpleDateFormat,flag:Int,calendar_type: String): String ={
    var types = Calendar.DATE//默认是天
    calendar_type.toLowerCase()match{

    }
  }

  def getMonth(date:Date,nday:Int): String ={
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar_date,nday)
  }


}
