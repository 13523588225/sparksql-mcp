package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.broadcast.Broadcast
import java.sql.{Connection, DriverManager}
import java.util.Properties

object mcp_fab_daily_volume_10mif {
  def main(args: Array[String]): Unit = {
    val sparkMysql: SparkSession = SparkSession.builder().getOrCreate()
    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_daily_volume_10mif")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 查询hive
    println("----查询Hive表----")
    val sourceDF: DataFrame = sparkHive.sql(
      """
        |select
        |	werk
        |	,spj
        |	,kanr
        |	,replace(factory,'PF','CP') plant
        |	,CASE
        |		WHEN status0 = 'M100' THEN 'M01'
        |		WHEN status0 = 'R100' THEN 'R1'
        |		WHEN status0 = 'R500' THEN 'R8'
        |		WHEN status0 = 'Z700' and COALESCE (vzgi,'OTHERS') != '743' THEN 'Z7'
        |		WHEN status0 = 'Z900' and COALESCE (vzgi,'OTHERS') != '743' THEN 'Z8'
        |		WHEN status0 = 'L100' THEN 'L1'
        |		WHEN status0 = 'L500' THEN 'L5'
        |		WHEN status0 = 'V900' THEN 'V900'
        |	END AS status0_t
        |	,time_slice
        |	,mdatum
        |	,mzeit
        |	,cal_date
        |from mcp.mcp_fab_veh_fh01t04_10mif
        |where mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |and plant is not null
        |and status0 in ('M100','R100''R500','Z700','Z900','L100','L500','V900','Z897','Z898')
        |and z89x_flag = 1
        |"""
        .stripMargin)



    //mysql url
    val url = "jdbc:mysql://10.122.48.84:4884/app"

    //配置你们数据库用户名和密码
    val properties = new Properties()
    properties.put("user", "mcp")
    properties.put("password", "3GlgK1IU6xL=")

    //    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    //    val table = "mcp_fab_daily_volume_10mif"
    //    sourceDF.write.mode("overwrite").jdbc(url, table, properties)

    val sql_delete = "delete from app.mcp_fab_daily_volume_10mif"
    val sql_insert = "insert into " +
      "app.mcp_fab_daily_volume_10mif(werk,spj,kanr,plant,status0_t,time_slice,mdatum,mzeit,cal_date) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    var conn:Connection = null

    sourceDF.foreachPartition { partition =>

      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(url, properties)
      conn.setAutoCommit(false)

      //删除数据
      conn.prepareStatement(sql_delete).execute()
      val stmt = conn.prepareStatement(sql_insert)

      println("时间戳：" + System.currentTimeMillis())
      try {
        partition.grouped(10000).foreach { batch =>

          batch.foreach { row =>
            stmt.setString(1, row.getString(0))
            stmt.setString(2, row.getString(1))
            stmt.setString(3, row.getString(2))
            stmt.setString(4, row.getString(3))
            stmt.setString(5, row.getString(4))
            stmt.setString(6, row.getString(5))
            stmt.setString(7, row.getString(6))
            stmt.setString(8, row.getString(7))
            stmt.setString(9, row.getString(8))
            stmt.addBatch()
          }
          stmt.executeBatch() //循环完后批次执行
        }
        //链接提交--->手动提交必须要关闭自动提交，默认事自动提交
        conn.commit()

      } catch {
        case e: Exception =>
          conn.rollback()
          throw e
      } finally {
        conn.close()
      }
    }
    sparkHive.stop()
  }
}
