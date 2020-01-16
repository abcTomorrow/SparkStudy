package com.wojiushiwo

import java.sql.DriverManager

/**
  * Created by myk
  * 2019/9/26 上午11:29
  */
object JdbcThriftserverDemo {
  def main(args: Array[String]): Unit = {

    val url:String="jdbc:hive2://192.168.7.16:10000"
    val name:String="hadoop"

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val connection = DriverManager.getConnection(url,name,"")

    val preparedStatement = connection.prepareStatement("select id,name from people")

    val resultSet = preparedStatement.executeQuery()

    while(resultSet.next()){
      println(resultSet.getInt("id")+"，"+resultSet.getString("name"))
    }

    resultSet.close()

    preparedStatement.close()

    connection.close()
  }
}
