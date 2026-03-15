package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_pf_modell_jph_cpy_df {
  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh01t04 = "ODS.FAB_FIS_90068_RPT_CPY_FH01T04_NT_STREAMING"
    val fh01t01 = "ODS.FAB_FIS_90066_RPT_CPY_FH01T01_NT_STREAMING"

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
        |		A.SPJ,
        |		B.KNR,
        |		A.WERK,
        |		B.MODELL,
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
        |		--通过T01表获取车型6位码
        |		SELECT KNR, WERK, SPJ, MODELL, FARBAU FROM fh01t01
        |	)B ON A.WERK = B.WERK AND A.SPJ = B.SPJ AND A.KNR1 = B.KNR
        |	LEFT JOIN
        |    -- 手工上传模块车型数据表
        |    ANALYTICAL_DB_MANUAL_TABLE.MCP_PF_CONTROL_CPY_MODELL_DF D
        |	ON B.MODELL = D.SIX_CODE
        |)
        |INSERT OVERWRITE TABLE mcp.mcp_pf_modell_jph_cpy_df
        |SELECT
        |	id,                                         -- 需求id
        |	description,								-- 模块说明
        |	concat(mdatumzeit,':00:00') mdatumzeit, 	-- M1过点日期
        |	carplant, 									-- 厂区名称
        |	count(1) jph,                               -- 小时产量
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') ETL_DATE	-- ETL数据计算时间
        |FROM
        |(
        |	-- 2. Passat WL HL（UD2）+Passat Pro HL/FS标配、CL选配（UD8）≤30JPH
        |	select
        |		id,
        |		description,
        |		carplant,
        |		spj,
        |		knr,
        |		werk,
        |		substr(mdatumzeit,1,13) mdatumzeit
        |	from tmp
        |	where modell = six_code and id = 2
        |	group by
        |		id,
        |		description,
        |		carplant,
        |		spj,
        |		knr,
        |		werk,
        |		substr(mdatumzeit,1,13)
        |	union all
        |	-- 3. Tharu XR≤30JPH
        |	select
        |		id,
        |		description,
        |		carplant,
        |		spj,
        |		knr,
        |		werk,
        |		substr(mdatumzeit,1,13) mdatumzeit
        |	from tmp
        |	where modell = six_code and id = 3
        |	group by
        |		id,
        |		description,
        |		carplant,
        |		spj,
        |		knr,
        |		werk,
        |		substr(mdatumzeit,1,13)
        |) T
        |group by
        |	id,
        |	description,
        |	carplant,
        |	concat(mdatumzeit,':00:00')
        |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
