package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import java.util.Properties

object mcp_fab_veh_dlq_latest_1day_hf {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val dt = args(0)
    println(s"开始日期: $dt")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_dlq_detail_sum_hf")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 查询hive
    println("----查询Hive表----")
    val sourceDF: DataFrame = sparkHive.sql(
      s"""
         |select  plant
         |       ,plant_date
         |       ,checkpoint
         |       ,plant_time_slice
         |       ,sum(hg) hg
         |       ,sum(zs) zs
         |       ,sum(torque_hg) torque_hg
         |       ,sum(torque_zs) torque_zs
         |from    mcp.mcp_fab_veh_dlq_detail_hi
         |where plant_date >= '${dt}'
         |and plant_date <= CURRENT_DATE()
         |group by plant
         |       ,checkpoint
         |       ,plant_date
         |       ,plant_time_slice
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
    val table = "mcp_fab_veh_dlq_latest_1day_hf"

    //数据全量写入mysql
    sourceDF.write.mode("overwrite").jdbc(url, table, props)

    // 创建数据库连接
    val connection = DriverManager.getConnection(url, props)

    // 调用mysql存储过程
    val callableStatement = connection.prepareCall("{call app.mcp_fab_veh_dlq_latest_hf_procedure()}")
    callableStatement.execute()

    inputStream.close()
    sparkHive.stop()
  }
}
