package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh04ta06_check_cph_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh04ta06 = "ods.fab_fis_90145_rpt_cph_fh04ta06_nt_streaming"
    val fh01tq45 = "ods.FAB_FIS_90137_RPT_CPH_FH01TQ45_NT_STREAMING"

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

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_fh04ta06_check_cph_hour")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |insert overwrite table mcp.mcp_fab_veh_fh04ta06_check_hour partition (werk, cal_date)
         |SELECT
         |	t1.spj
         |	,t1.kanr
         |	,t1.check_name
         |	,t1.capture_time
         |	,t2.text_ascii t_check_name
         |	,t1.check_value_id
         |	,t1.geraetename
         |	,t1.user_id
         |	,t1.kanr_mdatumzeit
         |	,t1.lastcheck
         |	,from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
         |	,t1.werk
         |	,substr(t1.capture_time,1,10) cal_date
         |FROM
         |(
         |	select * from fh04ta06
         |	where werk = 'C6'
         |	and capture_time >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 30 * 24 * 60 * 60,'yyyy-MM-dd')
         | and capture_time < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
         |)t1
         |left join
         |(
         |	select * from fh01tq45
         |	where   type = 'CHECK'
         |	and     lang_id = 'zh'
         |) t2
         |on t2.id = t1.check_name
         |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
