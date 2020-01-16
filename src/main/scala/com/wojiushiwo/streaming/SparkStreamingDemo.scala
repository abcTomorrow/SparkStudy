package com.wojiushiwo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by myk
  * 2019/10/12 上午10:29
  */
object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setAppName("SparkStreamingDemo")
      .setMaster("local[2]")
    //每一秒跑批一次
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    //处理socket数据
//    val lines=ssc.socketTextStream("localhost",9999)

    val lines= ssc.textFileStream("/Users/wojiushiwo/spark")

    val wordCounts=lines.flatMap(line=>line.split(" ")).map(word=>(word,1))
      .reduceByKey(_+_)

    wordCounts.print()

    //start the computation
    ssc.start()
    //Wait for the computation to terminate
    ssc.awaitTermination()
  }
}
