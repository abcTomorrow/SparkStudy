package com.wojiushiwo.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by myk
  * 2019/9/29 下午3:42
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ResolveFile")
      .config("spark.driver.host", "localhost")
      .master("local[2]")
      .getOrCreate();

    val df = spark.read.format("json").load("file:///Users/wojiushiwo/book/clean.json")
    df.printSchema()
    df.take(2).foreach(println)

    val day = "2017-05-11"
//    val topNVideoDataSet = videoAccessTopNStat(spark, df, day)
    // topNVideoDataSet.show(false)

       val orderByCityDataSet= videoOrderByCity(spark,df,day)
    //    orderByCityDataSet.show(false)

    spark.stop()
  }

  /**
    * 统计最受欢迎的TopN课程
    */
  def videoAccessTopNStat(spark: SparkSession, df: DataFrame, day: String) = {

    import spark.implicits._
    val target = df.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(count("cmsId") as ("nums"))
      .orderBy($"nums".desc)

    //        target.select(target.col("day"),target.col("cmsId"),target.col("nums")).show(false)

    //    df.createOrReplaceTempView("top")
    //    //两种方式求TopN课程
    //    spark.sql("select cmsId,count(cmsId) as nums from top where day='"+day+"' and cmsType='video' group by day,cmsId order by nums desc")


    try {
      target.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(row => {
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val times = row.getAs[Long]("nums")
          list.append(DayVideoAccessStat(day, cmsId, times))
        })
        StatDao.saveDayVideoAccessStat(list)
      })

    } catch {
      case e: Exception => e.printStackTrace()
    }


  }

  /**
    * 按地市统计最受欢迎的Top N课程
    */
  def videoOrderByCity(spark: SparkSession, df: DataFrame, day: String) {


    import spark.implicits._

    val cityAccessTopNDS = df.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId").agg(count("cmsId").as("times"))
      .orderBy(df.col("city").desc, $"times".desc)

    //    df.createOrReplaceTempView("top")

    //    spark.sql("select city,count（cmsId) as courseNums from top where day='"+day+"' and cmsType='video' group by city order by courseNums")

    val topDF = cityAccessTopNDS.select(
      cityAccessTopNDS("day"),
      cityAccessTopNDS("cmsId"),
      cityAccessTopNDS("city"),
      cityAccessTopNDS("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDS("city")).orderBy(cityAccessTopNDS("times").desc)).as("time_rank")
    )

    try {
      topDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(row => {
          val day = row.getAs[String]("day")
          val cmsId = row.getAs[Long]("cmsId")
          val city = row.getAs[String]("city")
          val times = row.getAs[Long]("times")
          val timesRank = row.getAs[Int]("time_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })
        StatDao.saveDayCityVideoAccessStat(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

}
