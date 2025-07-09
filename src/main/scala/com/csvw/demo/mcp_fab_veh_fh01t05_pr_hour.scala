package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh01t05_pr_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val meb_fh01t05 = "ods.fab_fis_90104_rpt_meb_fh01t05_nt_streaming"
    val cpn_fh01t05 = "ods.fab_fis_90115_rpt_cpn_fh01t05_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t05)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("meb_fh01t05")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpn_fh01t05)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("cpn_fh01t05")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      """
        |insert overwrite table mcp.mcp_fab_veh_fh01t05_pr_hour
        |select
        |	spj,
        |	werk,
        |	knr,
        |	CASE
        |		WHEN pnrstring LIKE '%SE2%' THEN 'SE2'
        |		WHEN pnrstring LIKE '%SV3%' THEN 'SV3'
        |		WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |		WHEN pnrstring LIKE '%SV6%' THEN 'SV6'
        |	END pr_nr,
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |from meb_fh01t05
        |union
        |select
        |	spj,
        |	werk,
        |	knr,
        |	CASE
        |		WHEN pnrstring LIKE '%SE1%' THEN 'SE1'
        |		WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |	END pr_nr,
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |from cpn_fh01t05
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
