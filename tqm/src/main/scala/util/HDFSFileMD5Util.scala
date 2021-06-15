package com.hongkuncc.bigdata.spark.Util

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.util.{Failure, Success, Try}

object HDFSFileMD5Util {
  def getHDFSFileMd5(path:String): String ={
    val fileSystem:FileSystem = FileSystem.newInstance(new Configuration())
    val in:FSDataInputStream = fileSystem.open(new Path(path))
    Try{
      DigestUtils.md5Hex(in)
    }match {
      case Success(s) => in.close();fileSystem.close();s;
      case Failure(e) => in.close();fileSystem.close();e.getMessage;
    }

  }

}
