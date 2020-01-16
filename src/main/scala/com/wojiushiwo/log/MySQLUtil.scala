package com.wojiushiwo.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by myk
  * 2019/9/30 下午10:20
  */
object MySQLUtil {

  def getConnection(): Connection = {
    //Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "root")
  }


  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(connection!=null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
  println(getConnection())
  }

}
