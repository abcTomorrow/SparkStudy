package com.wojiushiwo.log

import org.apache.spark.sql.SparkSession

/**
  * 1、数据清洗 从数据中找到自己需要的数据
  * Created by myk
  * 2019/9/29 下午12:54
  */
object ResolveFile {
  def main(args: Array[String]): Unit = {

    val filePath = "file:///Users/wojiushiwo/book/access_2.log"

    val spark = SparkSession.builder()
      .appName("ResolveFile")
      .config("spark.driver.host", "localhost")
      .master("local[2]")
      .getOrCreate();


    val fileRDD = spark.sparkContext.textFile(filePath)

//    fileRDD.take(10).foreach(println)
    //将数据处理 转存
    fileRDD.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)

      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)

      DateConvertUtil.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).coalesce(1).saveAsTextFile("file:///Users/wojiushiwo/book/output")
    //coalesce(1) 将产出的数据生成到一个文件中

    spark.stop()
  }
}
