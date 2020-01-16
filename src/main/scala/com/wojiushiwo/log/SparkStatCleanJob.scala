package com.wojiushiwo.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 2、对第一步生成的数据 再次处理 将url中的类目与id拆出来
  * 为了方便处理 使用access.log
  * Created by myk
  * 2019/9/29 下午2:32
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[2]")
      .getOrCreate()


    var accessRDD = spark.sparkContext.textFile("file:///Users/wojiushiwo/book/access_1.log")

    //    为什么accessRDD.map(_=>DataAccessUtil.convert(_)) 不能替换下面的一句话
    var df = spark.createDataFrame(accessRDD.map(x => DataAccessUtil.convert(x)), DataAccessUtil.structType)

    //产生文件中按某字段分区 那产生的数据结构中 便没有该字段
    //比如这里structType定义有day，而我们按day分区 那么数据文件中则不会有这个字段值
    df.coalesce(1).write
      .format("json")
      .mode(SaveMode.Overwrite)
      //        .partitionBy("day")
      .save("file:///Users/wojiushiwo/book/clean2")

    spark.stop()

  }
}
