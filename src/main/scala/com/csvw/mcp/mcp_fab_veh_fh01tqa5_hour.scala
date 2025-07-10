package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh01tqa5_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val cpy_fh01tqa5 = "ods.fab_fis_90069_rpt_cpy_fh01tqa5_nt_streaming"
    val meb_fh01tqa5 = "ods.fab_fis_90091_rpt_meb_fh01tqa5_nt_streaming"
    val cph_fh01tqa5 = "ods.fab_fis_90139_rpt_cph_fh01tqa5_nt_streaming"
    val cpc_fh01tqa5 = "ods.fab_fis_90163_rpt_cpc_fh01tqa5_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpy_fh01tqa5)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("cpy_fh01tqa5")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01tqa5)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("meb_fh01tqa5")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cph_fh01tqa5)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("cph_fh01tqa5")

    val kuduDF4: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01tqa5)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF4.createOrReplaceTempView("cpc_fh01tqa5")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
        |insert overwrite table mcp.mcp_fab_veh_fh01tqa5_hour
        |select
        |   werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select
        |		werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		ROW_NUMBER() over(PARTITION by werk, spj, kanr, module order by station_ts desc) rn
        |	from cpy_fh01tqa5
        |) a where meldekz not in ('HV','AV','HA') and rn = 1
        |union all
        |select werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select
        |		werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		ROW_NUMBER() over(PARTITION by werk, spj, kanr, module order by station_ts desc) rn
        |	from meb_fh01tqa5
        |) a where meldekz not in ('HV','AV','HA') and rn = 1
        |union all
        |select werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select
        |		werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		ROW_NUMBER() over(PARTITION by werk, spj, kanr, module order by station_ts desc) rn
        |	from cph_fh01tqa5
        |) a where meldekz not in ('HV','AV','HA') and rn = 1
        |union all
        |select werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select
        |		werk,
        |		spj,
        |		kanr,
        |		meldekz,
        |		module,
        |		station_ts,
        |		ROW_NUMBER() over(PARTITION by werk, spj, kanr, module order by station_ts desc) rn
        |	from cpc_fh01tqa5
        |) a where meldekz not in ('HV','AV','HA') and rn = 1
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
