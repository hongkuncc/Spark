package com.hongkuncc.bigdata.spark.Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import java.net.URI

object HDFSUtil {
  /*
  *
  * HDFS相关操作工具类
  *
  * */
  final val log = LogUtil.getLogger("HDFSUtil")

  //获取HDFS文件系统信息
  def getFileSystem(path:String):FileSystem={
    val conf = new Configuration()
    FileSystem.get(URI.create(path),conf)
  }

  //遍历目录
  def getFilesAndDir(path:String,fileSystem: FileSystem):Array[Path]={
    val fs = fileSystem.listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

}
