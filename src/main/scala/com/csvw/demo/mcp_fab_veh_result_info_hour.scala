package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_result_info_hour {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val cpc_fh01tqc0 = "ods.fab_fis_90183_rpt_cpc_fh01tqc0_nt_streaming"
    val cpc_fh01tqc1 = "ods.fab_fis_90184_rpt_cpc_fh01tqc1_nt_streaming"
    val cpc_fh01tq45 = "ods.fab_fis_90161_rpt_cpc_fh01tq45_nt_streaming"
    val cpc_fh01t04 = "ods.fab_fis_90175_rpt_cpc_fh01t04_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01tqc0)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("cpc_fh01tqc0")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01tqc1)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("cpc_fh01tqc1")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01tq45)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("cpc_fh01tq45")

    val kuduDF4: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF4.createOrReplaceTempView("cpc_fh01t04")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |insert overwrite table mcp.mcp_fab_veh_result_info_hour partition (werk, cal_date)
         |	select
         |		t1.spj,
         |		t1.kanr,
         |		t1.capture_time,
         |		t1.result_name,
         |		t1.result_value_id,
         |		t1.geraetename,
         |		t1.user_id,
         |		t2.value,
         |		t2.unit,
         |		t2.msr_value_id,
         |		t3.text_ascii,
         |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
         |        t1.werk,
         |		substr(t1.capture_time,1,10) cal_date
         |	from
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id from cpc_fh01tqc0
         |		where werk = 'CS'
         |        and capture_time >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 7 * 24 * 60 * 60,'yyyy-MM-dd')
         |        and capture_time < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
         |	) t1
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpc_fh01tqc1
         |        where werk = 'CS'
         |        and capture_time >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 7 * 24 * 60 * 60,'yyyy-MM-dd')
         |        and capture_time < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	(
         |		select * from cpc_fh01tq45
         |		where lang_id = 'zh'
         |	)t3 on t1.result_name = t3.id
         |	join cpc_fh01t04 t4
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr and t4.status0 = 'A700'
         |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
