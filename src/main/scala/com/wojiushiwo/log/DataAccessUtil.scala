package com.wojiushiwo.log

import com.ggstar.util.ip.IpHelper
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Created by myk
  * 2019/9/29 下午2:42
  */
object DataAccessUtil {

  val structType = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city",StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )


  /** 采用ipDataBase ip地址==>地区
    * https://github.com/wzhe06/ipdatabase
    * 采用ipDataBase ip地址==>地区
    * @param source
    * @return
    */
  def convert(source: String): Row = {
    try {
      val splits = source.split("\t")
      val url = splits(1)

      val separator = "http://www.imooc.com/";
      val newUrls = StringUtils.substringAfter(splits(1), separator).split("/")
      var cmsType = ""
      var cmsId = 0l
      if (newUrls.length > 1) {
        cmsType = newUrls(0)
        cmsId = newUrls(1).toLong
      }

      val traffic = splits(2).toLong
      val ip = splits(3)
      val city=IpHelper.findRegionByIp(ip)
      val time = splits(0)
      //取yyyy-MM-dd
      val day = StringUtils.substringBefore(time, " ")
      Row(url, cmsType, cmsId, traffic, ip,city, time, day)
    } catch {
      case e: Exception => Row(0)
    }

  }

  def main(args: Array[String]): Unit = {
    val date="2017-05-11 14:09:14"
    println( StringUtils.substringBefore(date, " "))
  }
}
