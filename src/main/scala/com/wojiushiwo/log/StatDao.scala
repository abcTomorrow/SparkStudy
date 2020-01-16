package com.wojiushiwo.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * Created by myk
  * 2019/9/30 下午10:35
  */
object StatDao {

  def saveDayVideoAccessStat(list:ListBuffer[DayVideoAccessStat])={

    var connection:Connection=null
    var pstmt:PreparedStatement=null

    try{
      connection=MySQLUtil.getConnection()
      connection.setAutoCommit(false)

      val sql="insert into day_video_access_stat(day,cms_id,times) values(?,?,?)"

      pstmt=connection.prepareStatement(sql)

      list.foreach(s=>{
        pstmt.setString(1,s.day)
        pstmt.setLong(2,s.cmsId)
        pstmt.setLong(3,s.times)

        pstmt.addBatch()
      })
      pstmt.executeBatch()
      connection.commit()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MySQLUtil.release(connection,pstmt)
    }

  }

def saveDayCityVideoAccessStat(list:ListBuffer[DayCityVideoAccessStat]): Unit ={

  var connection:Connection=null
  var ptsmt:PreparedStatement=null
  try{
    connection=MySQLUtil.getConnection()
    connection.setAutoCommit(false)

    val sql="insert into day_city_video_access_stat(day,cms_id,city,times,time_rank) values(?,?,?,?,?)"
    ptsmt=connection.prepareStatement(sql)
    list.foreach(s=>{
      ptsmt.setString(1,s.day)
      ptsmt.setLong(2,s.cmsId)
      ptsmt.setString(3,s.city)
      ptsmt.setLong(4,s.times)
      ptsmt.setInt(5,s.timesRank)

      ptsmt.addBatch()
    })

    ptsmt.executeBatch()
    connection.commit()

  }catch {
    case e:Exception=>e.printStackTrace()
  }finally {
    MySQLUtil.release(connection,ptsmt)
  }

}

}
