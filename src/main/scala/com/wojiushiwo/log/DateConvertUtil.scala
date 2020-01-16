package com.wojiushiwo.log

import java.util.{Date, Locale}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by myk 时间格式化工具
  * 2019/9/29 下午1:39
  */
object DateConvertUtil {

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 按照特定格式化 解析日期
    *
    * @param time
    * @return
    */
  def parse(time: String): String = {
    TARGET_FORMAT.format(getTime(time))
  }

  /**
    * 将形如[10/Nov/2016:00:01:02 +0800]的日期
    * 改为long类型的时间戳
    *
    * @param time
    */
  def getTime(time: String) = {
    //将字符串的[、]去掉
    try {
      val targetStr = StringUtils.replaceEach(time, Array("[", "]"), Array("", ""))
      YYYYMMDDHHMM_TIME_FORMAT.parse(targetStr).getTime
    } catch {
      case e: Exception => 0l
    }

  }

  def main(args: Array[String]): Unit = {
    var chars = Array("[", "]")

    println(StringUtils.replaceEach("[10/Nov/2016:00:01:02 +0800]", chars, Array("", "")))

    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
