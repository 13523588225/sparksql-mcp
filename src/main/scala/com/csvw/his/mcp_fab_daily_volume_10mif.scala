package com.csvw.his

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import java.util.Properties

object mcp_fab_daily_volume_10mif {
  def main(args: Array[String]): Unit = {
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
        |and status0 in ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898')
        |and z89x_flag = 1
        |"""
        .stripMargin)

    //获取配置文件
    val inputStream = getClass.getClassLoader.getResourceAsStream("mysql.properties")
    val props = new Properties
    // 加载配置文件
    props.load(inputStream)
    // 从配置文件中获取数据库连接信息
    val url = props.getProperty("url")
    //数据插入mysql表
    val table = "mcp_fab_daily_volume_10mif"
    sourceDF.write.mode("overwrite").jdbc(url, table, props)

    // 创建数据库连接
    val connection = DriverManager.getConnection(url, props)

    // 调用存储过程
    val callableStatement = connection.prepareCall("{call app.mcp_fab_daily_volume_10mif_procedure()}")
    callableStatement.execute()

    inputStream.close()
    sparkHive.stop()
  }
}
