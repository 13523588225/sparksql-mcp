package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import java.util.Properties

object mcp_fab_daily_buffer_hf {
  def main(args: Array[String]): Unit = {

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_daily_buffer_hf")
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
         |		max(case when status0 = 'R100' then mdatumzeit end) R1_TIME,
         |		max(case when status0 = 'R500' then mdatumzeit end) ZP5_TIME,
         |		max(case when status0 = 'L000' then mdatumzeit end) L000_TIME,
         |		max(case when status0 = 'L100' then mdatumzeit end) L100_TIME,
         |		max(case when status0 = 'L500' then mdatumzeit end) ZP5A_TIME,
         |		max(case when status0 = 'L800' then mdatumzeit end) L800_TIME,
         |		max(case when status0 = 'M100' then mdatumzeit end) M1_TIME,
         |		max(case when status0 = 'Z700' then mdatumzeit end) ZP7_TIME,
         |		-- 除 CPA3 CPY 工厂外，剔除其他工厂M7X0的数据
         |		max(case when status0 = 'M7X0' AND PLANT not in ('CPA3','CPY') THEN mdatumzeit end) M7X0_TIME,
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
         |		where STATUS0 IN ('R100','R500','L000','L100','L500','L800','M100','Z700','M7X0','Z900','V900')
         |		and VZGI = '766'
         |		union
         |		select
         |			plant,
         |			werk,
         |			spj,
         |			kanr,
         |			mdatumzeit,
         |			status0
         |		from
         |			mcp.mcp_fab_veh_fh01t04_hf
         |		where STATUS0 IN ('R100','R500','L000','L100','L500','L800','M100','Z700','M7X0','Z900','V900')
         |		and plant = 'CPH2'
         |		and VZGI = '766'
         |	) t
         |	group by PLANT, WERK, SPJ, KANR
         |)
         |INSERT OVERWRITE TABLE mcp.mcp_fab_daily_buffer_hf
         |SELECT distinct
         |	a.WERK,
         |	a.SPJ,
         |	a.KANR,
         |	a.PLANT,
         |	c.START_TIME,
         |	c.END_TIME,
         |	a.R1_TIME,
         |	a.ZP5_TIME,
         |	a.L000_TIME,
         |	a.L100_TIME,
         |	a.ZP5A_TIME,
         |	a.L800_TIME,
         |	a.M1_TIME,
         |	a.ZP7_TIME,
         |	a.M7X0_TIME,
         |	a.ZP8_TIME,
         |	a.V900_TIME,
         | from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
         |FROM
         |	tmp a
         |left join
         |(
         |	-- 剔除CPN转CPY、CPA3、CPH1的车辆
         |	select distinct source_plant plant, werk,spj,kanr from mcp.mcp_fab_veh_fh01t04_hf where is_skd = 1 union all
         |	-- 剔除CPH1转CPH2的车辆
         |	select 'CPH1' plant,werk,SPJ,KANR from tmp where  plant = 'CPH2' union all
         |	-- 剔除CPH2B2转CPH2B1的车辆
         |	select 'CPH2B2' plant,werk,SPJ,KANR from tmp where  plant = 'CPH2B1'
         |)b on a.plant = b.plant and a.KANR = b.KANR AND A.werk=B.werk AND A.spj = B.spj
         |left join
         |(
         |    select *,row_number() over(partition by factory order by start_date desc) rn from analytical_db_manual_table.mcp_pf_factory_work_schedule_df
         |) c on a.plant = c.factory and rn=1
         |where b.kanr is null and a.V900_TIME is null
        |"""
        .stripMargin)

    // 1.数据写入hive
    sourceDF.write.mode("overwrite").saveAsTable("mcp.mcp_fab_daily_buffer_10mif")
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
    val table = "mcp_fab_daily_buffer_hf"
    //数据全量写入mysql
    sourceDF
      .write
      .mode("overwrite")
      .option("batchsize", "10000") // 批量写入大小（减少连接次数）
      .option("truncate", "true") // 若mode=overwrite，truncate=true会先truncate表（保留表结构），false则drop后重建
      .jdbc(url, table, props)

    // 创建数据库连接
    val connection = DriverManager.getConnection(url, props)

    // 调用mysql存储过程
    val callableStatement = connection.prepareCall("{call app.mcp_fab_daily_buffer_hf_procedure()}")
    callableStatement.execute()

    println("mysql写入完成")

    inputStream.close()
    sparkHive.stop()
  }
}
