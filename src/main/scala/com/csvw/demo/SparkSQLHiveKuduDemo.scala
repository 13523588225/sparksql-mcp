package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLHiveKuduDemo {

  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051" // 替换为实际 Kudu Master 地址
    val meb_fh01t04 = "ods.fab_fis_90175_rpt_cpc_fh01t04_nt_streaming" // 替换为实际表名
    val meb_fh01t01 = "ods.fab_fis_90173_rpt_cpc_fh01t01_nt_streaming" // 替换为实际表名

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("meb_fh01t04")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("meb_fh01t01")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 计算kudu数据，导入hive
    println("----计算kudu数据，导入hive----")
    sparkHive.sql(
      """
        |insert overwrite table mcp.mcp_fh01t04_detail_tmp1
        |select t1.werk, t1.spj, t1.kanr, t1.mdatumzeit, t2.modell
        |from meb_fh01t04 t1
        |join meb_fh01t01 t2
        |on t1.werk=t2.werk and t1.spj=t2.spj and t1.kanr=t2.kanr0
        |where t1.mdatumzeit >= '2025-04-01'
        |"""
        .stripMargin)

    // 计算hive数据,导入hive
    println("----计算hive数据,导入hive----")
    sparkHive.sql(
      """
        |insert overwrite table mcp.mcp_fh01t04_detail_tmp2
        |select t1.werk, t1.spj, t1.kanr, t1.mdatumzeit, t1.modell, t2.modell series_name
        |from mcp.mcp_fh01t04_detail_tmp1 t1
        |join analytical_db_manual_table.mcp_pf_model_six_code_df t2
        |on t1.modell = t2.code_6
        |"""
        .stripMargin)

    // kudu hive混合计算
    println("----kudu hive混合计算----")
    sparkHive.sql(
      """
        |insert overwrite table mcp.mcp_fh01t04_detail_tmp
        |select t1.werk, t1.spj, t1.kanr, t1.mdatumzeit, t2.modell, t3.code_6
        |from meb_fh01t04 t1
        |join meb_fh01t01 t2
        |on t1.werk=t2.werk and t1.spj=t2.spj and t1.kanr=t2.kanr0
        |left join analytical_db_manual_table.mcp_pf_model_six_code_df t3
        |on t2.modell = t3.code_6
        |where t1.mdatumzeit >= '2025-03-01' and t3.code_6 is not null
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
