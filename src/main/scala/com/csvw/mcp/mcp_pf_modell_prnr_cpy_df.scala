package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_pf_modell_prnr_cpy_df {
  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh01t04 = "ODS.FAB_FIS_90068_RPT_CPY_FH01T04_NT_STREAMING"
    val fh01t01 = "ODS.FAB_FIS_90066_RPT_CPY_FH01T01_NT_STREAMING"
    val fh01t05 = "ODS.FAB_FIS_90055_RPT_CPY_FH01T05_NT_STREAMING"

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
      """
        |-- CPY 计算逻辑
        |with tmp as
        |(
        |	SELECT
        |		DENSE_RANK() OVER(ORDER BY MDATUMZEIT ASC) AS CAR_NUM,
        |		A.SPJ,
        |		B.KNR,
        |		A.WERK,
        |		B.MODELL,
        |		C.PR_UD2,
        |		C.PR_UD8,
        |		C.PR_8RC,
        |		C.PR_GK2,
        |		C.PR_6I5,
        |		C.PR_3T7,
        |		C.PR_4A4,
        |		C.PR_4D2,
        |		C.PR_UL1,
        |		C.PR_7Y1,
        |		C.PR_79H,
        |		C.PR_3FE,
        |		A.MDATUMZEIT,
        |		D.PLANT CARPLANT,
        |		D.ID,
        |		D.SIX_CODE,
        |		D.MODELL SERNAME,
        |		D.DESCRIPTION
        |	FROM
        |	(
        |	-- 只取过M100点的车
        |		SELECT SPJ, KNR1, WERK, MDATUMZEIT FROM fh01t04
        |		WHERE SUBSTR(KNR1,3,1) != '9'
        |		AND STATUS0 = 'M100'
        |		AND MDATUM >= FROM_UNIXTIME(UNIX_TIMESTAMP() - 60 * 24 * 60 * 60, 'yyyy-MM-dd')
        |	)A
        |    LEFT JOIN
        |	(
        |		-- 通过T01表获取车型6位码
        |		SELECT KNR, WERK, SPJ, MODELL, FARBAU FROM fh01t01
        |	)B ON A.WERK = B.WERK AND A.SPJ = B.SPJ AND A.KNR1 = B.KNR
        |	LEFT JOIN
        |	(
        |	-- 车辆基本信息
        |		SELECT
        |			SPJ,
        |			KNR,
        |			WERK,
        |			CASE WHEN PNRSTRING REGEXP 'UD2' THEN 'UD2' ELSE NULL END PR_UD2,
        |			CASE WHEN PNRSTRING REGEXP 'UD8' THEN 'UD8' ELSE NULL END PR_UD8,
        |			CASE WHEN PNRSTRING REGEXP '8RC' THEN '8RC' ELSE NULL END PR_8RC,
        |			CASE WHEN PNRSTRING REGEXP 'GK2' THEN 'GK2' ELSE NULL END PR_GK2,
        |			CASE WHEN PNRSTRING REGEXP '6I5' THEN '6I5' ELSE NULL END PR_6I5,
        |			CASE WHEN PNRSTRING REGEXP '3T7' THEN '3T7' ELSE NULL END PR_3T7,
        |			CASE WHEN PNRSTRING REGEXP '4A4' THEN '4A4' ELSE NULL END PR_4A4,
        |			CASE WHEN PNRSTRING REGEXP '4D2' THEN '4D2' ELSE NULL END PR_4D2,
        |			CASE WHEN PNRSTRING REGEXP 'UL1' THEN 'UL1' ELSE NULL END PR_UL1,
        |			CASE WHEN PNRSTRING REGEXP '7Y1' THEN '7Y1' ELSE NULL END PR_7Y1,
        |			CASE WHEN PNRSTRING REGEXP '79H' THEN '79H' ELSE NULL END PR_79H,
        |			CASE WHEN PNRSTRING REGEXP '3FE' THEN '3FE' ELSE NULL END PR_3FE
        |		FROM fh01t05
        |        WHERE PNRSTRING REGEXP 'UD2|UD8|8RC|GK2|6I5|3T7|4A4|4D2|UL1|7Y1|79H|3FE'
        |	) C ON A.SPJ = C.SPJ AND A.KNR1 = C.KNR AND A.WERK = C.WERK
        |	LEFT JOIN
        |    -- 手工上传模块车型数据表
        |    ANALYTICAL_DB_MANUAL_TABLE.MCP_PF_CONTROL_CPY_MODELL_DF D
        |	ON B.MODELL = D.SIX_CODE
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
        |	car_num,		-- 当前生产序列号
        |	last_car_num,	-- 上次车辆生产序列号
        |	dif_num,		-- 当前和上次生产序列号差值
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') ETL_DATE,	-- ETL数据计算时间
        |	carplant 		-- 厂区名称
        |FROM
        |(
        |	-- 1. Passat WL不连放
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where modell = six_code and id = 1
        |	union all
        |	-- 2. Passat WL HL（UD2）+Passat Pro HL/FS标配、CL选配（UD8）≤30JPH
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where modell = six_code and id = 2
        |	union all
        |	-- 3. Tharu XR≤30JPH
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where modell = six_code and id = 3
        |	union all
        |	-- 4. Passat Pro FS至少间隔2台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where modell = six_code and id = 4
        |	union all
        |	-- 5. Passat Pro FS标配/HL选配（8RC）至少间隔5台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where id = 5 and (six_code = 'R315MZ' or (six_code = 'R314MZ' and PR_8RC is not null))
        |	union all
        |	-- 6. Passat Pro HL/FS（GK2/6I5）至少间隔5台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where id = 6 and (PR_GK2 is not null or PR_6I5 is not null)
        |	union all
        |	-- 7. Passat Pro FS标配/HL选配（3T7/4A4/4D2/UL1）至少间隔5台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where id = 7 and (six_code = 'R315MZ' or (six_code = 'R314MZ' and (PR_3T7 is not null or PR_4A4 is not null or PR_4D2 is not null or PR_UL1 is not null)))
        |	union all
        |	-- 8. Tharu1.5T不能连放、Tharu1.5T与Passat Pro FS标配/HL选配（8RC）不能连放
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where id = 8 and (six_code in ('C823RZ','C824RZ','R315MZ') or (six_code = 'R314MZ' and PR_8RC is not null))
        |	union all
        |	-- 9. Passat WL HL标配/CL选配（7Y1）和Passat Pro CL/HL/FS（79H）连放不超过2台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where id = 9 and (six_code= 'C434SZ'
        |		or (six_code in('C433RZ','C433SZ') and PR_7Y1 is not null)
        |		or (six_code in ('R313EZ','R313MZ','R314MZ','R315MZ') and PR_79H is not null)
        |	)
        |	union all
        |	-- 10. Passat WL HL至少间隔5台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from tmp
        |	where modell = six_code and id = 10
        |	union all
        |	-- 11. Passat WL/Tharu XR任意组合连放不能超过2台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from
        |		tmp
        |	where modell = six_code and id = 11
        |	union all
        |	-- 12. Passat WL/Pro TL（3FE）至少间隔2台
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from
        |		tmp
        |	where id = 12 and PR_3FE is not null
        |	union all
        |	-- 13. Passat WL 280/330TSI与Passat WL 380TSI需要分开生产
        |	select
        |		description,
        |		carplant,
        |		sername,
        |		modell,
        |		spj,
        |		knr,
        |		werk,
        |		mdatumzeit,
        |		car_num,															 -- 当前序列号
        |		lag(car_num,  1,  0) over(order by mdatumzeit asc) last_car_num,	 -- 上次序列号
        |		car_num - lag(car_num,  1, 0) over(order by mdatumzeit asc) dif_num  -- 当前和上次序列号差值
        |	from
        |		tmp
        |	where modell = six_code and id = 13
        |) p
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
