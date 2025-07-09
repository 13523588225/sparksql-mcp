package com.csvw.demo

import org.apache.spark.sql.SparkSession

object SparkSQLHiveDemo {

  def main(args: Array[String]): Unit = {

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 查询hive
    println("----DataFrame API 读取 Hive 表----")
    sparkHive.table("analytical_db_manual_table.mcp_pf_model_six_code_df").show()

    // 查询hive
    println("----查询Hive表----")
    sparkHive.sql("select * From analytical_db_manual_table.mcp_pf_model_six_code_df")
      .show()

    sparkHive.stop()
  }
}
