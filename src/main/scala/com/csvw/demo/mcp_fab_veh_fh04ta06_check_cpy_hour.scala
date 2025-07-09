package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh04ta06_check_cpy_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val kuduDF5: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", "ods.fab_fis_90068_rpt_cpy_fh01t04_nt_streaming").option("kudu.master", kuduMaster).load().filter("status0 = 'A700'")
    val kuduDF6: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", "ods.fab_fis_90066_rpt_cpy_fh01t01_nt_streaming").option("kudu.master", kuduMaster).load()
    val kuduDF7: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", "ods.fab_fis_90076_rpt_cpy_fh04ta06_nt_streaming").option("kudu.master", kuduMaster).load().filter(s"capture_time >= '${bizdate}'")
    val kuduDF8: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu").option("kudu.table", "ods.fab_fis_90059_rpt_cpy_fh01tq45_nt_streaming").option("kudu.master", kuduMaster).load().filter("type = 'CHECK' and lang_id = 'zh'")

    kuduDF5.createOrReplaceTempView("cpy_fh01t04")
    kuduDF6.createOrReplaceTempView("cpy_fh01t01")
    kuduDF7.createOrReplaceTempView("cpy_fh04ta06")
    kuduDF8.createOrReplaceTempView("cpy_fh01tq45")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_fh04ta06_check_cpy_hour")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      """
        |insert overwrite table mcp.mcp_fab_veh_fh04ta06_check_hour2 partition (werk, cal_date)
        | SELECT
        |		t1.plant
        |		,t2.vin
        |		,COALESCE (t3.modell,'OTHERS') series_name_6
        |		,t2.modell
        |		,t4.spj
        |		,t4.kanr
        |		,t4.check_name
        |		,t4.capture_time
        |		,t4.plant_date
        |		,t5.text_ascii t_check_name
        |		,t4.check_value_id
        |		,t4.geraetename
        |		,t4.user_id
        |		,t4.kanr_mdatumzeit
        |		,t4.lastcheck
        |		,from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |		,t4.werk
        |		,substr(t4.capture_time,1,10) cal_date
        |	FROM
        |	(
        |		select
        |			'CPY' plant,
        |			werk,
        |			spj,
        |			kanr
        |		from cpy_fh01t04
        |	) t1
        |	join
        |	(
        |		--通过T01表获取车型6位码
        |		select knr, werk, spj, kanr0, modell, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpy_fh01t01
        |	) t2
        |	on t1.werk = t2.werk and t1.spj = t2.spj and t1.kanr = t2.kanr0
        |	left join
        |	analytical_db_manual_table.mcp_pf_model_six_code_df t3
        |	on t2.modell = t3.code_6
        |	join
        |	(
        |		select *,
        |		from_unixtime(unix_timestamp(substr(capture_time, 1, 19), 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd') plant_date from cpy_fh04ta06
        |	) t4
        |		on t1.werk = t4.werk and t1.spj = t4.spj and t1.kanr = t4.kanr
        |	left join
        |	cpy_fh01tq45 t5
        |	on t4.check_name = t5.id
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
