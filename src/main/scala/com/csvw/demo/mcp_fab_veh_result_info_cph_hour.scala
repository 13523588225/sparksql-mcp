package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_result_info_cph_hour {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val cph_fh01t01 = "ods.fab_fis_90150_rpt_cph_fh01t01_nt_streaming"
    val cph_fh01t04 = "ods.fab_fis_90152_rpt_cph_fh01t04_nt_streaming"
    val cph_fh01tq45 = "ods.fab_fis_90137_rpt_cph_fh01tq45_nt_streaming"
    val cph_fh01tqc0 = "ods.fab_fis_90185_rpt_cph_fh01tqc0_nt_streaming"
    val cph_fh01tqc1 = "ods.fab_fis_90186_rpt_cph_fh01tqc1_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF6: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF7: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01t04).option("kudu.master", kuduMaster).load()
    val kuduDF8: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tq45).option("kudu.master", kuduMaster).load()
    val kuduDF9: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tqc0).option("kudu.master", kuduMaster).load()
    val kuduDF10: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tqc1).option("kudu.master", kuduMaster).load()

    kuduDF6.createOrReplaceTempView("cph_fh01t01")
    kuduDF7.createOrReplaceTempView("cph_fh01t04")
    kuduDF8.createOrReplaceTempView("cph_fh01tq45")
    kuduDF9.createOrReplaceTempView("cph_fh01tqc0")
    kuduDF10.createOrReplaceTempView("cph_fh01tqc1")


    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |insert overwrite table mcp.mcp_fab_veh_result_info_hour2 partition (werk, cal_date)
         |-- CPH1 & CPH2
         | 	select distinct
         |		T4.plant,
         |		t5.vin,
         |		t5.modell series_code_6,
         |		COALESCE (t6.modell,'OTHERS') series_name_6,
         |		t1.spj,
         |		t1.kanr,
         |		t1.capture_time,
         |		t1.plant_date,
         |		t1.result_name,
         |		t1.result_value_id,
         |		t1.geraetename,
         |		t1.user_id,
         |		t2.value,
         |		t2.unit,
         |		t2.msr_value_id,
         |		t3.text_ascii,
         |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
         |		t1.werk,
         |		t1.cal_date
         |	from
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss') - 390 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from cph_fh01tqc0
         |		where capture_time >= '${bizdate}'
         |	) t1
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cph_fh01tqc1
         |		where capture_time >= '${bizdate}'
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	(
         |		select * from cph_fh01tq45
         |		where lang_id = 'zh'
         |	)t3 on t1.result_name = t3.id
         |	join
         |	(
         |		select
         |			'CPH2' plant,
         |			werk,
         |			spj,
         |			kanr
         |		from cph_fh01t04
         |		where status0 = 'A700'
         |		and substr(anlbgr3,-1,1) IN ('H','J','K')
         |	) t4
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cph_fh01t01
         |	) t5
         |		on t1.werk = t5.werk and t1.spj = t5.spj and t1.kanr = t5.kanr0
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |		on t5.modell = t6.code_6
         |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
