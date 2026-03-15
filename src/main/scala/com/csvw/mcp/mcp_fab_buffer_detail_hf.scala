package com.csvw.mcp

import org.apache.spark.sql.SparkSession

object mcp_fab_buffer_detail_hf {
  def main(args: Array[String]): Unit = {

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_buffer_detail_hf")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 查询hive
    println("----查询Hive表----")
    val sourceDF = sparkHive.sql(
      """
        |with tmp as
        |(
        |	select
        |		WERK,
        |		SPJ,
        |		KANR,
        |		PLANT,
        |		factory,
        |		max(case when status0 = 'R100' then mdatumzeit end) R1_TIME,
        |		max(case when status0 = 'R470' then mdatumzeit end) R470_TIME,
        |		max(case when status0 = 'R500' then mdatumzeit end) ZP5_TIME,
        |		max(case when status0 = 'L000' then mdatumzeit end) L000_TIME,
        |		max(case when status0 = 'L100' then mdatumzeit end) L100_TIME,
        |		max(case when status0 = 'L300' then mdatumzeit end) L300_TIME,
        |		max(case when status0 = 'L500' then mdatumzeit end) ZP5A_TIME,
        |		max(case when status0 = 'L800' then mdatumzeit end) L800_TIME,
        |		max(case when status0 = 'M100' then mdatumzeit end) M1_TIME,
        |		max(case when status0 = 'M200' then mdatumzeit end) M200_TIME,
        |		max(case when status0 = 'M300' then mdatumzeit end) M300_TIME,
        |		max(case when status0 = 'Z700' then mdatumzeit end) ZP7_TIME,
        |		max(case when status0 = 'M710' then mdatumzeit end) M710_TIME,
        |		max(case when status0 = 'M730' then mdatumzeit end) M730_TIME,
        |		max(case when status0 = 'M8X0' then mdatumzeit end) M8X0_TIME,
        |		max(case when status0 = 'M800' then mdatumzeit end) M800_TIME,
        |		max(case when status0 = 'Z900' then mdatumzeit end) ZP8_TIME,
        |		max(case when status0 = 'V900' then mdatumzeit end) V900_TIME
        |	from
        |	(
        |		select
        |			CASE
        |				WHEN werk = 'C6' AND substr(anlbgr3,-1,1) = 'J' THEN 'CPH2B1'
        |				WHEN werk = 'C6' AND substr(anlbgr3,-1,1) = 'K' THEN 'CPH2B2'
        |				else plant
        |			END plant,
        |			plant factory,
        |			werk,
        |			spj,
        |			kanr,
        |			mdatumzeit,
        |			-- CPH1的L100改成R700,其他工厂还是L100
        |			CASE
        |				WHEN plant = 'CPH1' AND status0 = 'L100' THEN NULL
        |				WHEN plant = 'CPH1' AND status0 = 'R700' THEN 'L100'
        |				ELSE status0
        |			END status0
        |		from
        |			mcp.mcp_fab_veh_fh01t04_hf
        |		where STATUS0 IN ('R100','R470','R500','L000','L100','L300','L500','L800','M100','M200','M300','Z700','M710','M730','M8X0','M800','Z900','V900')
        |		and VZGI = '766'
        |		union
        |		select
        |			plant,
        |			plant factory,
        |			werk,
        |			spj,
        |			kanr,
        |			mdatumzeit,
        |			status0
        |		from
        |			mcp.mcp_fab_veh_fh01t04_hf
        |		where STATUS0 IN ('R100','R470','R500','L000','L100','L300','L500','L800','M100','M200','M300','Z700','M710','M730','M8X0','M800','Z900','V900')
        |		and plant = 'CPH2'
        |		and VZGI = '766'
        |	) t
        |	group by PLANT,factory, WERK, SPJ, KANR
        |)
        |insert overwrite table mcp.mcp_fab_buffer_detail_hf
        |SELECT distinct
        |	a.WERK,
        |	a.SPJ,
        |	a.KANR,
        |	a.PLANT,
        |	c.START_TIME,
        |	c.END_TIME,
        |	a.R1_TIME,
        |	a.R470_TIME,
        |	a.ZP5_TIME,
        |	a.L000_TIME,
        |	a.L100_TIME,
        |	a.L300_TIME,
        |	a.ZP5A_TIME,
        |	a.L800_TIME,
        |	a.M1_TIME,
        |	a.M200_TIME,
        |	a.M300_TIME,
        |	a.ZP7_TIME,
        |	a.M710_TIME,
        |	a.M730_TIME,
        |	a.M8X0_TIME,
        |	a.M800_TIME,
        |	a.ZP8_TIME,
        |	a.V900_TIME,
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |FROM
        |	tmp a
        |left join
        |(
        |	-- 剔除SKD车辆
        |	select distinct source_plant plant, werk,spj,kanr from mcp.mcp_fab_veh_fh01t04_hf where is_skd = 1 union all
        |	-- 剔除CPH1转CPH2的车辆
        |	select 'CPH1' plant,werk,SPJ,KANR from tmp where  plant = 'CPH2' union all
        |	-- 剔除CPH2B2转CPH2B1的车辆
        |	select 'CPH2B2' plant,werk,SPJ,KANR from tmp where  plant = 'CPH2B1'
        |)b on a.plant = b.plant and a.KANR = b.KANR AND A.werk=B.werk AND A.spj = B.spj
        |left join
        |(
        |    select *,row_number() over(partition by factory order by start_date desc) rn from analytical_db_manual_table.mcp_pf_factory_work_schedule_df
        |) c on a.factory = c.factory and rn=1
        |where b.kanr is null and a.V900_TIME is null
        |"""
        .stripMargin)

    sparkHive.stop()
  }
}
