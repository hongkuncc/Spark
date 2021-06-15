package com.hongkuncc.bigdata.spark.Util

import org.apache.spark.sql.SparkSession

object UdfUtil {

  //完全脱敏隐私，包括数值生日，另一要求是出现业务不符合和未记录的
  def tranHide(spark:SparkSession): Unit ={
    spark.udf.register("transDate",(str:String)=>"9999-12-31")
  }

  //脱敏转换银行账号
  def transBank(spark:SparkSession): Unit ={
    spark.udf.register("transBank",(str:String)=>{
      var name:String = ""
      if(str == "未记录"||str == "未采集"||str == "公司未采集"||str =="公司未记录"||str == "不适用")name = str
      if(str != null && str.length<=3 && str.length !=0)name = str.substring(str.length-1)else name =str
      name
    })
  }

}
