package com.csvw.his

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_shift_info_cpc_hour {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val fh25tw02 = "ods.fab_fis_90171_rpt_cpc_fh25tw02_nt_streaming"
    val fh25tw03 = "ods.fab_fis_90172_rpt_cpc_fh25tw03_nt_streaming"
    val fh25tw12 = "ods.fab_fis_90187_rpt_cpc_fh25tw12_nt_streaming"
    val source = "CPC"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh25tw02)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("fh25tw02")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh25tw03)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("fh25tw03")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", fh25tw12)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("fh25tw12")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
         |insert overwrite table mcp.mcp_fab_veh_shift_info_hour partition (source,shiftdate)
         |SELECT
         |	CASE
         |		WHEN plantcode = 'PFM' then 'CPM'
         |		WHEN plantcode = 'PFA2' then 'CPA2'
         |		WHEN plantcode = 'PFA3' then 'CPA3'
         |		WHEN plantcode = 'PFH1' or (plantcode='PFH2' and shopname='PFHB') then 'CPH1'
         |		WHEN plantcode = 'PFH2' and shopname<>'PFHB' then 'CPH2'
         |		WHEN plantcode = 'PFY' then 'CPY'
         |		WHEN plantcode = 'PFW' then 'CPW'
         |		WHEN plantcode = 'PFN' then 'CPN'
         |		WHEN plantcode = 'PFC' then 'CPC'
         |	END AS plant
         |	,CASE
         |		WHEN plantcode = 'PFH1' or (plantcode = 'PFH2' and shopname = 'PFHB') then 'PFH1'
         |		WHEN plantcode = 'PFH2' and shopname <> 'PFHB' then 'PFH2'
         |		WHEN shopname in ('PFA3A1','PFA3B1') then 'PFA3 L1'
         |		WHEN shopname in ('PFA3A2','PFA3B2') then 'PFA3 L2'
         |		ELSE plantcode
         |	END AS factory
         |	,plantcode
         |	,shopname
         |	,CASE
         |		WHEN shopname LIKE 'PFA%' OR shopname LIKE 'PFH%' THEN substring(shopname,1,5)
         |		ELSE substring(shopname,1,4)
         |	END AS shopname_s
         |	,CASE
         |		when substring(shopname,4,1) IN ('A','B','P') then substring(shopname,4,1)
         |		when substring(shopname,5,1) IN ('A','B','P') then substring(shopname,5,1)
         |	end as shopname_t
         |	,seqno
         |	,starttime
         |	,endtime
         |	,CASE
         |		WHEN ( UNIX_TIMESTAMP(END_TIME) - UNIX_TIMESTAMP(START_TIME) ) / 3600 <= 4 THEN round(( UNIX_TIMESTAMP(END_TIME) - UNIX_TIMESTAMP(START_TIME) ) / 3600, 2)
         |		WHEN ( UNIX_TIMESTAMP(END_TIME) - UNIX_TIMESTAMP(START_TIME) ) / 3600 > 4  THEN round(( UNIX_TIMESTAMP(END_TIME) - UNIX_TIMESTAMP(START_TIME) ) / 3600 - 0.5 , 2)
         |	END AS workinghours
         |	,start_time
         |	,end_time
         |	,shiftname
         |	,replace(regexp_replace(shiftname,'\\/|\\~|\\&|\\@|\\#|\\^|\\=|A|B|C|L|T',''),'*','') shiftname_new
         |	,shiftid
         |	,isneedbreak
         |	,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
         |	,'$source' source
         |	,shiftdate
         |FROM
         |(
         |	SELECT
         |		 A.SHIFTID
         |		,A.PLANTCODE
         |		,A.ISNEEDBREAK
         |		,C.SHOPNAME
         |		,A.SHIFTNAME
         |		,B.STARTTIME
         |		,B.ENDTIME
         |		,B.SEQNO
         |		,C.SHIFTDATE
         |		,CONCAT(C.SHIFTDATE, ' ', B.STARTTIME) START_TIME
         |		,CASE WHEN B.ENDTIME > B.STARTTIME THEN CONCAT(C.SHIFTDATE, ' ', B.ENDTIME)
         |		ELSE from_unixtime(unix_timestamp(CONCAT(C.SHIFTDATE, ' ', B.ENDTIME),'yyyy-MM-dd HH:mm:ss') + 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
         |		END END_TIME
         |	FROM fh25tw02 A
         |	LEFT JOIN fh25tw03 B ON A.SHIFTID = B.SHIFTID
         |	LEFT JOIN fh25tw12 C ON A.SHIFTID = C.SHIFTID
         |) t1
         |where SHIFTDATE >= trunc(add_months(from_unixtime(unix_timestamp('${bizdate}', 'yyyyMMdd'), 'yyyy-MM-dd'), -(1)), 'MM')
         |"""
        .stripMargin)

    sparkKudu.stop()
    sparkHive.stop()
  }
}
