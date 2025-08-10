package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh04ta06_check_hi {
  def main(args: Array[String]): Unit = {

    val tb_name = args(0)
    println(s"数据输出表: $tb_name")
    // 开始日期
    val start_date = args(1)
    val end_date = args(2)
    println(s"开始日期: $start_date")
    println(s"结束日期: $end_date")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"
    val fh01tqc = Map(
      "cpc_fh04ta06" -> "ods.fab_fis_90169_rpt_cpc_fh04ta06_nt_streaming",
      "cpy_fh04ta06" -> "ods.fab_fis_90076_rpt_cpy_fh04ta06_nt_streaming",
      "cph_fh04ta06" -> "ods.fab_fis_90145_rpt_cph_fh04ta06_nt_streaming",
      "meb_fh04ta06" -> "ods.fab_fis_90097_rpt_meb_fh04ta06_nt_streaming"
    )

    // 动态注册所有表
    fh01tqc.map { case (targetTable, sourceTable) =>
      val df = sparkKudu.read
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", sourceTable)
        .load()
        .filter(s"substr(capture_time,1,10) >= '${start_date}' and substr(capture_time,1,10) <= '${end_date}'")

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")
      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

    val fh01tq45 = Map(
      "cpc_fh01tq45" -> "ods.fab_fis_90161_rpt_cpc_fh01tq45_nt_streaming",
      "cph_fh01tq45" -> "ods.fab_fis_90137_rpt_cph_fh01tq45_nt_streaming",
      "cpy_fh01tq45" -> "ods.fab_fis_90059_rpt_cpy_fh01tq45_nt_streaming",
      "meb_fh01tq45" -> "ods.fab_fis_90089_rpt_meb_fh01tq45_nt_streaming"
    )

    // 动态注册所有表
    fh01tq45.map { case (targetTable, sourceTable) =>
      val df = sparkKudu.read
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", sourceTable)
        .load()
        .filter("type = 'CHECK' and lang_id = 'zh'")

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")
      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_fh04ta06_check_hi")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    sparkHive.table("mcp.mcp_fab_veh_fh01t04_hf")
      .filter("status0 = 'A700'")
      .createOrReplaceTempView("fh01t04")

    // 输出到hive
    sparkHive.sql(
      s"""
        | insert overwrite table $tb_name partition (werk, cal_date)
        |SELECT
        |	t0.plant
        |	,t0.vin
        |	,t0.series_name_6
        |    ,t0.modell
        |	,t1.spj
        |	,t1.kanr
        |	,t1.check_name
        |	,t1.capture_time
        |	,case
        |		when t1.capture_time >= concat(t1.cal_date,' ', t0.start_time)
        |			and t1.capture_time < concat(from_unixtime(unix_timestamp(t1.cal_date,'yyyy-MM-dd') + 86400, 'yyyy-MM-dd'),' ', t0.end_time) then t1.cal_date
        |		else from_unixtime(unix_timestamp(t1.cal_date,'yyyy-MM-dd') - 86400, 'yyyy-MM-dd')
        |	end plant_date
        |	,t2.text_ascii t_check_name
        |	,t1.check_value_id
        |	,t1.geraetename
        |	,t1.user_id
        |	,t1.kanr_mdatumzeit
        |	,t1.lastcheck
        |	,from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |	,t1.werk
        |	,t1.cal_date
        |FROM
        |(
        |	select
        |		case when plant = 'CPH1' then 'CPH2' else plant end plant,start_time,end_time,
        |		vin,modell,series_name series_name_6, werk,spj,kanr from fh01t04
        |	where status0 = 'A700'
        |) t0
        |join
        |(
        |	select *,substr(capture_time,1,10) cal_date from cpc_fh04ta06
        |	union all
        |	select *,substr(capture_time,1,10) cal_date from cpy_fh04ta06
        |	union all
        |	select *,substr(capture_time,1,10) cal_date from cph_fh04ta06
        |	union all
        |	select *,substr(capture_time,1,10) cal_date from meb_fh04ta06
        |)t1
        |	on t1.werk = t0.werk and t1.spj = t0.spj and t1.kanr = t0.kanr
        |left join
        |(
        |	select '78' werk,* from meb_fh01tq45
        |	union all
        |	select 'CS' werk, * from cpc_fh01tq45
        |	union all
        |	select 'C5' werk, * from cpy_fh01tq45
        |	union all
        |	select 'C6' werk, * from cph_fh01tq45
        |)t2
        |on t1.check_name = t2.id and t1.werk = t2.werk
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
