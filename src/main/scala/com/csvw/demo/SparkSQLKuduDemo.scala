package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLKuduDemo {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession，启用 Kudu 扩展
    val spark: SparkSession = SparkSession.builder()
      .appName("Kudu Opration Demo")
      .getOrCreate()

    // Kudu Master 地址配置
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051" // 替换为实际地址
    val sourceTable = "mcp.mcp_cpc_fh01t04_test"
    val targetTable = "mcp.mcp_cpc_fh01t04_test_bak"

    // 读source表
    val kuduDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTable)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("source_view")

    // 获取 source 数据（可在此处添加过滤或转换逻辑）
    val sourceDF = spark.sql(
      """
        |select werk, spj, kanr, mdatum, mzeit, werk0, status0,fanlage2,geraetename3,
        |from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |from source_view limit 10
        |""".stripMargin)

    // 核心写入操作：upsert 模式
    sourceDF.write
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.master", kuduMaster)
      .option("kudu.table", targetTable)
      .mode("append") // 固定为 append 模式（与 operation 配合使用）
      .save()

    spark.stop()
  }
}
