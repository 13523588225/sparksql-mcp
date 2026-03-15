package com.csvw.mcp

import org.apache.spark.sql.SparkSession

import java.util.Properties

object mcp_fab_buffer_overview_hf {
  def main(args: Array[String]): Unit = {

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_buffer_overview_hf")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 查询hive
    println("----查询Hive表----")
    val sourceDF = sparkHive.sql(
      """
        |select
        |	plant
        |	,checkpoint
        |	,nvl(sum(ist),0) ist
        |	,nvl(sum(buffer),0) buffer
        |	,nvl(sum(work_station),0) work_station
        |	,cast(nvl(sum(target),0) as decimal(28,6)) target
        |	,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select
        |		plant
        |		,checkpoint
        |		,ist
        |		,null target
        |		,null buffer
        |		,null work_station
        |	from
        |	(
        |		select  plant
        |			,'R1 - R470' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where r1_time is not null
        |		and r470_time is null
        |		and zp5_time  is null
        |		and l000_time is null
        |		and l100_time is null
        |		and l300_time is null
        |		and zp5a_time is null
        |		and l800_time is null
        |		and m1_time   is null
        |		and m200_time is null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'R470 - ZP5' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where r470_time is not null
        |		and zp5_time  is null
        |		and l000_time is null
        |		and l100_time is null
        |		and l300_time is null
        |		and zp5a_time is null
        |		and l800_time is null
        |		and m1_time   is null
        |		and m200_time is null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'L100 - L300' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where l100_time is not null
        |		and l300_time is null
        |		and zp5a_time is null
        |		and l800_time is null
        |		and m1_time   is null
        |		and m200_time is null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'L300 - ZP5a' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where l300_time is not null
        |		and zp5a_time is null
        |		and l800_time is null
        |		and m1_time   is null
        |		and m200_time is null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M1 - M200' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m1_time is not null
        |		and m200_time is null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M200 - M300' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m200_time is not null
        |		and m300_time is null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M300 - ZP7' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m300_time is not null
        |		and zp7_time  is null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'ZP7 - M710' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where zp7_time is not null
        |		and m710_time is null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M710 - M730' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m710_time is not null
        |		and m730_time is null
        |		and m8x0_time is null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M730 - M800' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m730_time is not null
        |		and m800_time is null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M800 - ZP8' checkpoint
        |			,count(kanr) ist
        |		from mcp.mcp_fab_buffer_detail_hf
        |		where m800_time is not null
        |		and zp8_time  is null
        |		and v900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'R1 - ZP5' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   r1_time is not null
        |		and     ZP5_TIME is null
        |		and     L100_time is null
        |		and     L800_TIME is null
        |		and     M1_time is null
        |		and     ZP7_time is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'ZP5 - L100' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   ZP5_time is not null
        |		and     L100_time is null
        |		and     L800_TIME is null
        |		and     M1_time is null
        |		and     ZP7_time is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'L100 - ZP5a' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   L100_time is not null
        |		and     L800_TIME is null
        |		and     M1_time is null
        |		and     ZP7_time is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'ZP5a - M1' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   L800_time is not null
        |		and     M1_time is null
        |		and     ZP7_time is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'M1 - ZP7' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   M1_time is not null
        |		and     ZP7_time is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'ZP7 - ZP8' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   ZP7_time is not null
        |		AND     M8X0_TIME is null
        |		and     ZP8_time is null
        |		and     V900_time is null
        |		group by plant
        |		union all
        |		select  plant
        |			,'ZP8 - V900' checkpoint
        |			,count(kanr) ist
        |		from    mcp.mcp_fab_buffer_detail_hf
        |		where   ZP8_time is not null
        |		and     V900_time is null
        |		group by plant
        |    ) a
        |	union all
        |	-- 计划值
        |	SELECT
        |		plant
        |		,checkpoint
        |		,null ist
        |		,round(nvl(sum(case when stations_type = 'buffer' then soll end)/2,0) + nvl(sum(case when stations_type = 'work station' then soll end),0),0) target
        |		,sum(case when stations_type = 'buffer' then soll end) buffer
        |		,sum(case when stations_type = 'work station' then soll end) work_station
        |	From    analytical_db_manual_table.mcp_pf_buffer_data_list_df
        |	where stations_type in ('buffer','work station')
        |	group by plant ,checkpoint
        |) t
        |where plant in ('CPC','CPY','CPH1','CPH2','CPA2','CPA3','CPM')
        |group by plant ,checkpoint
        |"""
        .stripMargin)

    // 1.数据写入hive
    sourceDF.write.mode("overwrite").saveAsTable("mcp.mcp_fab_buffer_overview_hf")
    println("hive写入完成")

    // 2.数据写入mysql
    //获取mysql配置文件
    val inputStream = getClass.getClassLoader.getResourceAsStream("mysql.properties")
    val props = new Properties
    // 加载配置文件
    props.load(inputStream)
    // 从配置文件中获取数据库连接信息
    val url = props.getProperty("url")
    //数据插入mysql表
    val table = "mcp_fab_buffer_overview_hf"
    //数据全量写入mysql
    sourceDF
      .write
      .mode("overwrite")
      .option("batchsize", "10000") // 批量写入大小（减少连接次数）
      .option("truncate", "true") // 若mode=overwrite，truncate=true会先truncate表（保留表结构），false则drop后重建
      .jdbc(url, table, props)

    println("mysql写入完成")

    inputStream.close()
    sparkHive.stop()
  }
}
