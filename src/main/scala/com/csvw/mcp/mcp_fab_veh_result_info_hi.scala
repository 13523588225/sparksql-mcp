package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat

object mcp_fab_veh_result_info_hi {
  def main(args: Array[String]): Unit = {

    val tb_name = args(0)
    println(s"数据输出表: $tb_name")
    // 开始日期
    val start_date = args(1)
    val end_date = args(2)
    println(s"开始日期: $start_date")
    println(s"结束日期: $end_date")

    val t1 = System.currentTimeMillis()
    val ds1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t1)

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh01tqc = Map(
      "cpc_fh01tqc0" -> "ods.fab_fis_90183_rpt_cpc_fh01tqc0_nt_streaming",
      "cph_fh01tqc0" -> "ods.fab_fis_90185_rpt_cph_fh01tqc0_nt_streaming",
      "cpy_fh01tqc0" -> "ods.fab_fis_90195_rpt_cpy_fh01tqc0_nt_streaming",
      "meb_fh01tqc0" -> "ods.fab_fis_90196_rpt_meb_fh01tqc0_nt_streaming",
      "cpc_fh01tqc1" -> "ods.fab_fis_90184_rpt_cpc_fh01tqc1_nt_streaming",
      "cph_fh01tqc1" -> "ods.fab_fis_90186_rpt_cph_fh01tqc1_nt_streaming",
      "cpy_fh01tqc1" -> "ods.fab_fis_90194_rpt_cpy_fh01tqc1_nt_streaming",
      "meb_fh01tqc1" -> "ods.fab_fis_90197_rpt_meb_fh01tqc1_nt_streaming"
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
        .filter("lang_id = 'zh'")

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")
      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

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

    sparkHive.table("mcp.mcp_fab_veh_fh01t04_hf")
      .filter("status0 = 'A700'")
      .createOrReplaceTempView("fh01t04")

    // 输出到hive
    sparkHive.sql(
      s"""
         |	insert overwrite table $tb_name partition (werk, cal_date)
         | 	select distinct
         |		t0.plant,
         |		t0.vin,
         |		t0.series_code_6,
         |		t0.series_name_6,
         |		t1.spj,
         |		t1.kanr,
         |		t1.capture_time,
         |		case
         |			when t1.capture_time >= concat(t1.cal_date,' ', t0.start_time)
         |				and t1.capture_time < concat(from_unixtime(unix_timestamp(t1.cal_date, 'yyyy-MM-dd') + 86400, 'yyyy-MM-dd'),' ', t0.end_time) then t1.cal_date
         |			else from_unixtime(unix_timestamp(t1.cal_date, 'yyyy-MM-dd') - 86400, 'yyyy-MM-dd')
         |		end plant_date,
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
         |			case when plant = 'CPH1' then 'CPH2' else plant end plant, start_time, end_time,
         |			vin,modell series_code_6,series_name series_name_6, werk,spj,kanr from fh01t04
         |		where status0 = 'A700'
         |	) t0
         |	join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id, substr(capture_time,1,10) cal_date from meb_fh01tqc0
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id, substr(capture_time,1,10) cal_date from cpc_fh01tqc0
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id, substr(capture_time,1,10) cal_date from cpy_fh01tqc0
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,result_value_id,geraetename,user_id, substr(capture_time,1,10) cal_date from cph_fh01tqc0
         |	) t1
         |		on t0.werk = t1.werk and t0.spj = t1.spj and t0.kanr = t1.kanr
         |	left join
         |	(
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from meb_fh01tqc1
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpc_fh01tqc1
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cpy_fh01tqc1
         |		union all
         |		select werk,spj,kanr,capture_time,result_name,unit,value,msr_value_id from cph_fh01tqc1
         |	) t2
         |		on t2.werk = t1.werk and t2.spj = t1.spj and t2.kanr = t1.kanr and t1.result_name = t2.result_name
         |		and substr( t2.capture_time,1,21) = substr( t1.capture_time,1,21)
         |	left join
         |	(
         |	  select '78' werk,* from meb_fh01tq45
         |	  union all
         |	  select 'CS' werk, * from cpc_fh01tq45
         |	  union all
         |	  select 'C5' werk, * from cpy_fh01tq45
         |	  union all
         |	  select 'C6' werk, * from cph_fh01tq45
         |	)t3 on t1.result_name = t3.id and t1.werk = t3.werk
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
