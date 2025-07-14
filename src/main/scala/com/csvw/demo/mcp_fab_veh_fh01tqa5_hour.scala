package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh01tqa5_hour {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val dt = args(0)
    println(s"开始日期: $dt")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val tablelist = Map(
      "cpy_fh01tqa5" -> "ods.fab_fis_90069_rpt_cpy_fh01tqa5_nt_streaming",
      "meb_fh01tqa5" -> "ods.fab_fis_90091_rpt_meb_fh01tqa5_nt_streaming",
      "cph_fh01tqa5" -> "ods.fab_fis_90139_rpt_cph_fh01tqa5_nt_streaming",
      "cpc_fh01tqa5" -> "ods.fab_fis_90163_rpt_cpc_fh01tqa5_nt_streaming"
    )

    // 动态注册所有表
    tablelist.map { case (targetTable, sourceTable) =>
      val df = sparkKudu.read
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", sourceTable)
        .load()
        .filter(s"station_ts >= '${dt}'")

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")

      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

    // 执行多表计算
    val sql: String =
      """
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
        |""".stripMargin

    // 输出到hive
    sparkKudu.sql(sql)

    sparkKudu.stop()
  }
}
