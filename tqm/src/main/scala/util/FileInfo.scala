package com.hongkuncc.bigdata.spark.Util

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object FileInfo {
  val spark = new InitialSparkSession().getSparkSession("FileInfo")
  def getFileInfo(path:String,fileSystem: FileSystem):Map[String,String]={

    //文件路径
    var mapFile = Map[String,String]()
    val listPath =
  }

}
