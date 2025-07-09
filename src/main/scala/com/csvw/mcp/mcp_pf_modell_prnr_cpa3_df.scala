package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_pf_modell_prnr_cpa3_df {
  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh01t04 = "ods.fab_fis_90103_rpt_meb_fh01t04_nt_streaming"
    val fh01t01 = "ods.fab_fis_90102_rpt_meb_fh01t01_nt_streaming"
    val fh01t05 = "ods.fab_fis_90104_rpt_meb_fh01t05_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("fh01t04")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("fh01t01")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh01t05)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("fh01t05")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |with tmp as
         |(
         |	select
         |		row_number() over(order by mdatumzeit asc) as car_num,
         |		a.SPJ,
         |		b.KNR,
         |		a.WERK,
         |		B.modell,
         |		C.PR_1X1,
         |		A.mdatumzeit,
         |		'CPA3' carplant,
         |		d.six_code modell_s,
         |		d.modell sername,
         |		d.description
         |	from
         |	(
         |	-- 只取过M100点的车
         |		select spj, knr1, werk, mdatumzeit from fh01t04
         |		where substr(knr1,3,1) != '9'
         |		and fanlage2 like '%CP3%'
         |		and status0 = 'M100'
         |		and mdatum >= from_unixtime(unix_timestamp() - 60 * 24 * 60 * 60, 'yyyy-MM-dd')
         |	)a
         |    left join
         |	(
         |		--通过T01表获取车型6位码
         |		select knr, werk, spj, modell from fh01t01
         |	)b on a.werk = b.werk and a.spj = b.spj and a.knr1 = b.knr
         |	left join
         |	(
         |	-- 车辆基本信息
         |		select
         |			SPJ,
         |			KNR,
         |			WERK,
         |			'1X1' PR_1X1
         |		from fh01t05
         |        where pnrstring like '%1X1%'
         |	)c on a.spj = c.spj and a.knr1 = c.knr and a.werk = c.werk
         |	left JOIN
         |    -- 手工上传模块车型数据表
         |    analytical_db_manual_table.mcp_pf_control_cpa3_modell_df d
         |	on b.modell = d.six_code
         |)
         |INSERT OVERWRITE TABLE mcp.mcp_pf_modell_prnr_df partition (carplant)
         |SELECT
         |	description,	-- 模块说明
         |	sername,		-- 车系名称
         |	modell,			-- 车系六位码
         |	spj, 			-- 订单号年份
         |	knr, 			-- 订单号
         |	werk,			-- 厂区
         |	mdatumzeit, 	-- M1过点时间
         |	null car_kind, 	-- 汽车类别：大车、小车、未说明为空(null)
         |	null colour,    -- 颜色代码
         |	car_num,		    -- 当前生产序列号
         |	last_car_num,	  -- 上次车辆生产序列号
         |	dif_num,		    -- 当前和上次生产序列号差值
         |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') ETL_DATE, -- ETL数据计算时间
         |	carplant 		-- 厂区名称
         |FROM
         |(
         |	-- 1. 最少间隔2
         |	select
         |		description,
         |		carplant,
         |		sername,
         |		modell,
         |		spj,
         |		knr,
         |		werk,
         |		mdatumzeit,
         |		car_num,					-- 当前序列号
         |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,		-- 上次序列号
         |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
         |	from tmp
         |	where modell = modell_s and description = '最少间隔2'
         |	union all
         |	-- 2. 最少间隔5
         |	select
         |		description,
         |		carplant,
         |		sername,
         |		modell,
         |		spj,
         |		knr,
         |		werk,
         |		mdatumzeit,
         |		car_num,					-- 当前大车生产序列号
         |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,		-- 上次大车生产序列号
         |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次生产序列号差值
         |	from tmp
         |	where modell = modell_s and description = '最少间隔5'
         |	union all
         |	-- 3. 最少间隔1
         |	select
         |		description,
         |		carplant,
         |		sername,
         |		modell,
         |		spj,
         |		knr,
         |		werk,
         |		mdatumzeit,
         |		car_num,					-- 当前大车生产序列号
         |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,		-- 上次大车生产序列号
         |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次生产序列号差值
         |	from tmp
         |	where modell = modell_s and description = '最少间隔1'
         |	union all
         |	-- PR-NR 1X1
         |	select
         |		'1X1' description,
         |		carplant,
         |		sername,
         |		modell,
         |		spj,
         |		knr,
         |		werk,
         |		mdatumzeit,
         |		car_num,					-- 当前大车生产序列号
         |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,		-- 上次大车生产序列号
         |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次生产序列号差值
         |	from tmp
         |	where PR_1X1 is not null
         |) T
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
