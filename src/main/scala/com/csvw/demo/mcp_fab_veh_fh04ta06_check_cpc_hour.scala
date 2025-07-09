package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh04ta06_check_cpc_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh04ta06 = "ods.fab_fis_90169_rpt_cpc_fh04ta06_nt_streaming"
    val fh01tq45 = "ods.fab_fis_90161_rpt_cpc_fh01tq45_nt_streaming"
    val fh01t01 = "ods.fab_fis_90173_rpt_cpc_fh01t01_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh04ta06)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("fh04ta06")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh01tq45)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("fh01tq45")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("fh01t01")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_fh04ta06_check_cpc_hour")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |insert overwrite table mcp.mcp_fab_veh_fh04ta06_check_hour partition (werk, cal_date)
         |SELECT
         |	a.spj
         |	,a.kanr
         |	,c.modell
         |	,a.check_name
         |	,a.capture_time
         |	,b.text_ascii t_check_name
         |	,a.check_value_id
         |	,a.geraetename
         |	,a.user_id
         |	,a.kanr_mdatumzeit
         |	,a.lastcheck
         |	,from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
         |	,a.werk
         |	,substr(a.capture_time,1,10) cal_date
         |FROM
         |(
         |	select * from fh04ta06
         |	where werk = 'CS'
         |	and capture_time >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 30 * 24 * 60 * 60,'yyyy-MM-dd')
         | and capture_time < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
         |)a
         |left join
         |(
         |	select * from fh01tq45
         |	where   type = 'CHECK'
         |	and     lang_id = 'zh'
         |) b
         |on b.id = a.check_name
         |left join
         |fh01t01 c
         |on a.werk = c.werk and a.spj = c.spj and a.kanr = c.kanr0
         |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
