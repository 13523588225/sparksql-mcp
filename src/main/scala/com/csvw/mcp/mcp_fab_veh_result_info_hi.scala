package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat

object mcp_fab_veh_result_info_hi {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val t1 = System.currentTimeMillis()
    val ds1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t1)

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val cpc_fh01t01 = "ods.fab_fis_90173_rpt_cpc_fh01t01_nt_streaming"
    val cpc_fh01t04 = "ods.fab_fis_90175_rpt_cpc_fh01t04_nt_streaming"
    val cpc_fh01tq45 = "ods.fab_fis_90161_rpt_cpc_fh01tq45_nt_streaming"
    val cpc_fh01tqc0 = "ods.fab_fis_90183_rpt_cpc_fh01tqc0_nt_streaming"
    val cpc_fh01tqc1 = "ods.fab_fis_90184_rpt_cpc_fh01tqc1_nt_streaming"
    val cph_fh01t01 = "ods.fab_fis_90150_rpt_cph_fh01t01_nt_streaming"
    val cph_fh01t04 = "ods.fab_fis_90152_rpt_cph_fh01t04_nt_streaming"
    val cph_fh01tq45 = "ods.fab_fis_90137_rpt_cph_fh01tq45_nt_streaming"
    val cph_fh01tqc0 = "ods.fab_fis_90185_rpt_cph_fh01tqc0_nt_streaming"
    val cph_fh01tqc1 = "ods.fab_fis_90186_rpt_cph_fh01tqc1_nt_streaming"
    val cpn_fh01t01 = "ods.fab_fis_90112_rpt_cpn_fh01t01_nt_streaming"
    val cpn_fh01t04 = "ods.fab_fis_90114_rpt_cpn_fh01t04_nt_streaming"
    val cpn_fh01tq45 = "ods.fab_fis_90125_rpt_cpn_fh01tq45_nt_streaming"
    val cpn_fh01tqc0 = "ods.fab_fis_90193_rpt_cpn_fh01tqc0_nt_streaming"
    val cpn_fh01tqc1 = "ods.fab_fis_90192_rpt_cpn_fh01tqc1_nt_streaming"
    val cpy_fh01t01 = "ods.fab_fis_90066_rpt_cpy_fh01t01_nt_streaming"
    val cpy_fh01t04 = "ods.fab_fis_90068_rpt_cpy_fh01t04_nt_streaming"
    val cpy_fh01tq45 = "ods.fab_fis_90059_rpt_cpy_fh01tq45_nt_streaming"
    val cpy_fh01tqc0 = "ods.fab_fis_90195_rpt_cpy_fh01tqc0_nt_streaming"
    val cpy_fh01tqc1 = "ods.fab_fis_90194_rpt_cpy_fh01tqc1_nt_streaming"
    val meb_fh01t01 = "ods.fab_fis_90102_rpt_meb_fh01t01_nt_streaming"
    val meb_fh01t04 = "ods.fab_fis_90103_rpt_meb_fh01t04_nt_streaming"
    val meb_fh01tq45 = "ods.fab_fis_90089_rpt_meb_fh01tq45_nt_streaming"
    val meb_fh01tqc0 = "ods.fab_fis_90196_rpt_meb_fh01tqc0_nt_streaming"
    val meb_fh01tqc1 = "ods.fab_fis_90197_rpt_meb_fh01tqc1_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF1: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpc_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpc_fh01t04).option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpc_fh01tq45).option("kudu.master", kuduMaster).load().filter("lang_id = 'zh'")
    val kuduDF4: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpc_fh01tqc0).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF5: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpc_fh01tqc1).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF6: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF7: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01t04).option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF8: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tq45).option("kudu.master", kuduMaster).load().filter("lang_id = 'zh'")
    val kuduDF9: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tqc0).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF10: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cph_fh01tqc1).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF11: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpn_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF12: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpn_fh01t04).option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF13: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpn_fh01tq45).option("kudu.master", kuduMaster).load().filter("lang_id = 'zh'")
    val kuduDF14: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpn_fh01tqc0).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF15: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpn_fh01tqc1).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF16: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpy_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF17: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpy_fh01t04).option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF18: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpy_fh01tq45).option("kudu.master", kuduMaster).load().filter("lang_id = 'zh'")
    val kuduDF19: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpy_fh01tqc0).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF20: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", cpy_fh01tqc1).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF21: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", meb_fh01t01).option("kudu.master", kuduMaster).load()
    val kuduDF22: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", meb_fh01t04).option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF23: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", meb_fh01tq45).option("kudu.master", kuduMaster).load().filter("lang_id = 'zh'")
    val kuduDF24: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", meb_fh01tqc0).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF25: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", meb_fh01tqc1).option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")

    // Kudu表注册
    kuduDF1.createOrReplaceTempView("cpc_fh01t01")
    kuduDF2.createOrReplaceTempView("cpc_fh01t04")
    kuduDF3.createOrReplaceTempView("cpc_fh01tq45")
    kuduDF4.createOrReplaceTempView("cpc_fh01tqc0")
    kuduDF5.createOrReplaceTempView("cpc_fh01tqc1")
    kuduDF6.createOrReplaceTempView("cph_fh01t01")
    kuduDF7.createOrReplaceTempView("cph_fh01t04")
    kuduDF8.createOrReplaceTempView("cph_fh01tq45")
    kuduDF9.createOrReplaceTempView("cph_fh01tqc0")
    kuduDF10.createOrReplaceTempView("cph_fh01tqc1")
    kuduDF11.createOrReplaceTempView("cpn_fh01t01")
    kuduDF12.createOrReplaceTempView("cpn_fh01t04")
    kuduDF13.createOrReplaceTempView("cpn_fh01tq45")
    kuduDF14.createOrReplaceTempView("cpn_fh01tqc0")
    kuduDF15.createOrReplaceTempView("cpn_fh01tqc1")
    kuduDF16.createOrReplaceTempView("cpy_fh01t01")
    kuduDF17.createOrReplaceTempView("cpy_fh01t04")
    kuduDF18.createOrReplaceTempView("cpy_fh01tq45")
    kuduDF19.createOrReplaceTempView("cpy_fh01tqc0")
    kuduDF20.createOrReplaceTempView("cpy_fh01tqc1")
    kuduDF21.createOrReplaceTempView("meb_fh01t01")
    kuduDF22.createOrReplaceTempView("meb_fh01t04")
    kuduDF23.createOrReplaceTempView("meb_fh01tq45")
    kuduDF24.createOrReplaceTempView("meb_fh01tqc0")
    kuduDF25.createOrReplaceTempView("meb_fh01tqc1")

    val t2 = System.currentTimeMillis()
    val ds2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t2)
    val dif = (t2 - t1) / 1000

    println(s"ds1时间: $ds1")
    println(s"ds2时间: $ds2")
    println(s"读取时间差(s): $dif")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |	insert overwrite table mcp.mcp_fab_veh_result_info_hi partition (werk, cal_date)
         |	-- CPC
         |	select distinct
         |		t4.plant,
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
         |		select
         |			'CPC' plant,
         |			werk,
         |			spj,
         |			kanr,
         |			knr1
         |		from cpc_fh01t04
         |	) t4
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpc_fh01t01
         |	) t5
         |	on t4.werk = t5.werk and t4.spj = t5.spj and t4.knr1 = t5.knr
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |	on t5.modell = t6.code_6
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss')- 330 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from cpc_fh01tqc0
         |	) t1
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpc_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	cpc_fh01tq45 t3
         |	on t1.result_name = t3.id
         |    union all
         |	-- CPY
         | 	select distinct
         |		t4.plant,
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
         |		select
         |			'CPY' plant,
         |			werk,
         |			spj,
         |			kanr,
         |			knr1
         |		from cpy_fh01t04
         |	) t4
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpy_fh01t01
         |	) t5
         |	on t4.werk = t5.werk and t4.spj = t5.spj and t4.knr1 = t5.knr
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |	on t5.modell = t6.code_6
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from cpy_fh01tqc0
         |	) t1
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpy_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	cpy_fh01tq45 t3
         |		on t1.result_name = t3.id
         |	union all
         |	-- CPN
         | 	select distinct
         |		t4.plant,
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
         |		select
         |			'CPN' plant,
         |			werk,
         |			spj,
         |			kanr,
         |			knr1
         |		from cpn_fh01t04
         |	) t4
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpn_fh01t01
         |	) t5
         |	on t4.werk = t5.werk and t4.spj = t5.spj and t4.knr1 = t5.knr
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |	on t5.modell = t6.code_6
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss') - 390 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from cpn_fh01tqc0
         |	) t1
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpn_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	cpn_fh01tq45 t3
         |		on t1.result_name = t3.id
         |	union all
         |	-- CPH1 & CPH2
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
         |		select
         |			'CPH2' plant,
         |			werk,
         |			spj,
         |			kanr,
         |			knr1
         |		from cph_fh01t04
         |		where substr(anlbgr3,-1,1) IN ('H','J','K')
         |	) t4
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cph_fh01t01
         |	) t5
         |		on t4.werk = t5.werk and t4.spj = t5.spj and t4.knr1 = t5.knr
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |		on t5.modell = t6.code_6
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from cph_fh01tqc0
         |	) t1
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cph_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	cph_fh01tq45 t3
         |		on t1.result_name = t3.id
         |	union all
         |	-- CPA2 & CPA3 & CPM
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
         |		select
         |			CASE
         |				WHEN fanlage2 like '%CP2%' THEN 'CPA2'
         |				WHEN fanlage2 like '%CP3%' THEN 'CPA3'
         |				WHEN fanlage2 like '%MEB%' THEN 'CPM'
         |			END plant,
         |			werk,
         |			spj,
         |			kanr,
         |			knr1
         |		from meb_fh01t04
         |	) t4
         |	join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from meb_fh01t01
         |	) t5
         |		on t4.werk = t5.werk and t4.spj = t5.spj and t4.knr1 = t5.knr
         |	left join
         |	analytical_db_manual_table.mcp_pf_model_six_code_df t6
         |		on t5.modell = t6.code_6
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id,
         |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd') plant_date,
         |		substr(capture_time,1,10) cal_date
         |		from meb_fh01tqc0
         |	) t1
         |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from meb_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	meb_fh01tq45 t3 on t1.result_name = t3.id
         |"""
        .stripMargin)

    val t3 = System.currentTimeMillis()
    val ds3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t3)
    val dif2 = (t3 - t2) / 1000

    println(s"ds2时间: $ds2  --  ds3时间: $ds3")
    println(s"读取时间差(s): $dif2")

    sparkKudu.stop()
    sparkHive.stop()
  }
}
