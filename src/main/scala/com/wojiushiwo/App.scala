package com.wojiushiwo


import org.apache.spark.sql.SparkSession
/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    import spark.implicits._

//
//    val df = spark.read.json(args(0))
//    df.show()
//    //    /println(spark)
//    df.printSchema()
//    df.select()
//    df.select("name").show()
//
//    df.select($"name",$"age"+1).show()
//    df.filter($"age">21).show()
//    df.groupBy("age").count().show()
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("select * from people")
//    sqlDF.show()

//    case class Person(name:String,age:Long)
//    val dataFrame = spark.sparkContext.textFile("/Users/wojiushiwo/json/people.text")
//      .map(_.split(","))
//      .map(s => Person(s(0), s(1).trim().toInt))
//      .toDF()

//    val peopleRDD = spark.sparkContext.textFile("/Users/wojiushiwo/json/people.text")
//    val schemaString="name age"
//    val fields=schemaString
//      .split(" ")
//      .map(fieldName=>StructField(fieldName,StringType,nullable = true))
//
//    val schema=StructType(fields)
//
//    val rowRDD=peopleRDD.map(_.split(","))
//      .map(attributes=>Row(attributes(0),attributes(1).trim))
//
//    val peopleDF=spark.createDataFrame(rowRDD,schema)
//
//    peopleDF.show()

    val dataFrame = spark.read.format("json")
      .json("/Users/wojiushiwo/json/people.json")


    dataFrame.createOrReplaceTempView("people")

//    spark.sql("select * from people").where("age>19").show(false)
    val frame = spark.sql("select * from people")
    frame.select(frame.col("name")).show()

    import spark.implicits._
    //RDD ==>DataFrame
    spark.sparkContext.textFile("/Users/wojiushiwo/json/people.txt")
        .map(_.split(","))
        .map(s=>People(s(0),s(1).trim.toInt))
      .toDF()

    spark.stop()
  }

  case class People(name:String,age:Int)
}
