package com.wojiushiwo

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by myk
  * 2019/9/27 下午4:02
  */
object HiveAndJdbcDemo {
  def main(args: Array[String]): Unit = {

   val spark= SparkSession.builder()
      .appName("HiveAndJdbcDemo")
      .master("local[2]")
      .getOrCreate()
    // 这句话 在本地无法运行成功 因为本地并未有任何hive服务启动 需要放到spark-shell上运行
    val hiveDept=spark.table("dept")

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")
    properties.setProperty("driver","com.mysql.jdbc.Driver")

    val url="jdbc:mysql://192.168.7.2:3306"

    val mysqlDept = spark.read.jdbc(url,"sparksql.dept",properties)
    //join 这里是inner join;可以根据需要改为outer、left等
    val resultDept=hiveDept.join(mysqlDept,hiveDept.col("deotno")===mysqlDept.col("deptno"))

    resultDept.select(hiveDept.col("deptno"),mysqlDept.col("dname")).show(false)

  }
}
