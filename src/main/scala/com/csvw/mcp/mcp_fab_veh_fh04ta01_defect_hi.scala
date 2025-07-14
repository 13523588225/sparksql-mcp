package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh04ta01_defect_hi {
  def main(args: Array[String]): Unit = {
    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val tablelist_fis = Map(
      "cpc_fh04ta01" -> "ods.fab_fis_90188_rpt_cpc_fh04ta01_nt_streaming",
      "cph_fh04ta01" -> "ods.fab_fis_90189_rpt_cph_fh04ta01_nt_streaming",
      "cpy_fh04ta01" -> "ods.fab_fis_90193_rpt_cpy_fh04ta01_nt_streaming",
      "meb_fh04ta01" -> "ods.fab_fis_90194_rpt_meb_fh04ta01_nt_streaming",
      "cpc_fh01tq68" -> "ods.fab_fis_90162_rpt_cpc_fh01tq68_nt_streaming",
      "cph_fh01tq68" -> "ods.fab_fis_90138_rpt_cph_fh01tq68_nt_streaming",
      "cpy_fh01tq68" -> "ods.fab_fis_90073_rpt_cpy_fh01tq68_nt_streaming",
      "meb_fh01tq68" -> "ods.fab_fis_90090_rpt_meb_fh01tq68_nt_streaming",
      "cpc_fh01tq09" -> "ods.fab_fis_90182_rpt_cpc_fh01tq09_nt_streaming",
      "cph_fh01tq09" -> "ods.fab_fis_90133_rpt_cph_fh01tq09_nt_streaming",
      "cpy_fh01tq09" -> "ods.fab_fis_90075_rpt_cpy_fh01tq09_nt_streaming",
      "meb_fh01tq09" -> "ods.fab_fis_90085_rpt_meb_fh01tq09_nt_streaming",
      "cpc_fh01tq07" -> "ods.fab_fis_90181_rpt_cpc_fh01tq07_nt_streaming",
      "cph_fh01tq07" -> "ods.fab_fis_90135_rpt_cph_fh01tq07_nt_streaming",
      "cpy_fh01tq07" -> "ods.fab_fis_90072_rpt_cpy_fh01tq07_nt_streaming",
      "meb_fh01tq07" -> "ods.fab_fis_90084_rpt_meb_fh01tq07_nt_streaming",
      "cpc_fh01tqe8" -> "ods.fab_fis_90190_rpt_cpc_fh01tqe8_nt_streaming",
      "cph_fh01tqe8" -> "ods.fab_fis_90191_rpt_cph_fh01tqe8_nt_streaming",
      "cpy_fh01tqe8" -> "ods.fab_fis_90052_rpt_cpy_fh01tqe8_nt_streaming",
      "meb_fh01tqe8" -> "ods.fab_fis_90053_rpt_meb_fh01tqe8_nt_streaming",
      "cpc_fh01tq11" -> "ods.fab_fis_90158_rpt_cpc_fh01tq11_nt_streaming",
      "cph_fh01tq11" -> "ods.fab_fis_90132_rpt_cph_fh01tq11_nt_streaming",
      "cpy_fh01tq11" -> "ods.fab_fis_90071_rpt_cpy_fh01tq11_nt_streaming",
      "meb_fh01tq11" -> "ods.fab_fis_90086_rpt_meb_fh01tq11_nt_streaming"
    )

    // 动态注册所有表
    tablelist_fis.map { case (targetTable, sourceTable) =>
      val df = sparkKudu.read
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", sourceTable)
        .load()

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")

      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

    // 输出到hive
    sparkKudu.sql(
      s"""
        |insert overwrite table mcp.mcp_fab_veh_fh04ta01_defect_hi partition (werk, cal_date)
        |select
        |	t01.spj,
        |	t01.kanr,
        |	t01.defect_ts,
        |	t01.defectstate,
        |	t01.defectclose_ts,
        |	t01.coordinate_x,
        |	t01.coordinate_y,
        |	t01.geraetename_nio,
        |	t01.geraetename_io,
        |	t01.user_id_nio,
        |	t01.user_id_io,
        |	t01.eqs_reading_open,
        |	t01.eqs_reading_close,
        |	t01.capturetime_nio,
        |	t01.capturetime_io,
        |	dc2.maingroup_text_asc,
        |	dc3.type_text_ascii,
        |	dc4.loc_text_ascii,
        |	t4.pos_text_ascii,
        |	dc5.locpos_text_ascii,
        |	t01.repair_ts,
        |	t01.repair_nio_ts,
        |	t01.geraetename_repair,
        |	t01.geraetename_repair_nio,
        |	t01.user_id_repair,
        |	t01.user_id_repair_nio,
        |	from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
        |	t01.werk,
        |	substr(t01.defect_ts,1,10) cal_date
        |from
        |(
        |	select * from cpc_fh04ta01
        |	where defect_ts >= trunc(add_months(from_unixtime(unix_timestamp('${bizdate}', 'yyyyMMdd'), 'yyyy-MM-dd'), -(1)), 'MM')
        | and defect_ts < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
        |) t01
        |left join
        |(
        |	SELECT  DISTINCT maingroup_id
        |					,maingroup_text_asc
        |	FROM    cpc_fh01tq68
        |	WHERE   MAINGROUP_LANG = 'zh'
        |) dc2
        |on      t01.supergroup_id = dc2.maingroup_id
        |left join
        |(
        |	SELECT  DISTINCT type_id
        |					,type_text_ascii
        |	FROM    cpc_fh01tq09
        |	WHERE   TYPE_LANG = 'zh'
        |) dc3
        |on      t01.type_id = dc3.type_id
        |left join
        |(
        |	SELECT  DISTINCT loc_id
        |					,loc_text_ascii
        |	FROM    cpc_fh01tq07
        |	WHERE   LOC_LANG = 'zh'
        |) dc4
        |on t01.loc_id = dc4.loc_id
        |left join
        |(
        |	SELECT  DISTINCT locpos_id
        |					,locpos_text_ascii
        |	FROM    cpc_fh01tqe8
        |	WHERE   LOCPOS_LANG = 'zh'
        |) dc5
        |on      t01.locpos_id = dc5.locpos_id
        |LEFT join cpc_fh01tq11 t4
        |on      t01.pos_id = t4.pos_id
        |union all
        |select
        |	t01.spj,
        |	t01.kanr,
        |	t01.defect_ts,
        |	t01.defectstate,
        |	t01.defectclose_ts,
        |	t01.coordinate_x,
        |	t01.coordinate_y,
        |	t01.geraetename_nio,
        |	t01.geraetename_io,
        |	t01.user_id_nio,
        |	t01.user_id_io,
        |	t01.eqs_reading_open,
        |	t01.eqs_reading_close,
        |	t01.capturetime_nio,
        |	t01.capturetime_io,
        |	dc2.maingroup_text_asc,
        |	dc3.type_text_ascii,
        |	dc4.loc_text_ascii,
        |	t4.pos_text_ascii,
        |	dc5.locpos_text_ascii,
        |	t01.repair_ts,
        |	t01.repair_nio_ts,
        |	t01.geraetename_repair,
        |	t01.geraetename_repair_nio,
        |	t01.user_id_repair,
        |	t01.user_id_repair_nio,
        |	from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
        |	t01.werk,
        |	substr(t01.defect_ts,1,10) cal_date
        |from
        |(
        |	select * from cph_fh04ta01
        |	where defect_ts >= trunc(add_months(from_unixtime(unix_timestamp('${bizdate}', 'yyyyMMdd'), 'yyyy-MM-dd'), -(1)), 'MM')
        | and defect_ts < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
        |) t01
        |left join
        |(
        |	SELECT  DISTINCT maingroup_id
        |					,maingroup_text_asc
        |	FROM    cph_fh01tq68
        |	WHERE   MAINGROUP_LANG = 'zh'
        |) dc2
        |on      t01.supergroup_id = dc2.maingroup_id
        |left join
        |(
        |	SELECT  DISTINCT type_id
        |					,type_text_ascii
        |	FROM    cph_fh01tq09
        |	WHERE   TYPE_LANG = 'zh'
        |) dc3
        |on      t01.type_id = dc3.type_id
        |left join
        |(
        |	SELECT  DISTINCT loc_id
        |					,loc_text_ascii
        |	FROM    cph_fh01tq07
        |	WHERE   LOC_LANG = 'zh'
        |) dc4
        |on t01.loc_id = dc4.loc_id
        |left join
        |(
        |	SELECT  DISTINCT locpos_id
        |					,locpos_text_ascii
        |	FROM    cph_fh01tqe8
        |	WHERE   LOCPOS_LANG = 'zh'
        |) dc5
        |on      t01.locpos_id = dc5.locpos_id
        |LEFT join cph_fh01tq11 t4
        |on      t01.pos_id = t4.pos_id
        |union all
        |select
        |	t01.spj,
        |	t01.kanr,
        |	t01.defect_ts,
        |	t01.defectstate,
        |	t01.defectclose_ts,
        |	t01.coordinate_x,
        |	t01.coordinate_y,
        |	t01.geraetename_nio,
        |	t01.geraetename_io,
        |	t01.user_id_nio,
        |	t01.user_id_io,
        |	t01.eqs_reading_open,
        |	t01.eqs_reading_close,
        |	t01.capturetime_nio,
        |	t01.capturetime_io,
        |	dc2.maingroup_text_asc,
        |	dc3.type_text_ascii,
        |	dc4.loc_text_ascii,
        |	t4.pos_text_ascii,
        |	dc5.locpos_text_ascii,
        |	t01.repair_ts,
        |	t01.repair_nio_ts,
        |	t01.geraetename_repair,
        |	t01.geraetename_repair_nio,
        |	t01.user_id_repair,
        |	t01.user_id_repair_nio,
        |	from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
        |	t01.werk,
        |	substr(t01.defect_ts,1,10) cal_date
        |from
        |(
        |	select * from cpy_fh04ta01
        |	where defect_ts >= trunc(add_months(from_unixtime(unix_timestamp('${bizdate}', 'yyyyMMdd'), 'yyyy-MM-dd'), -(1)), 'MM')
        | and defect_ts < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
        |) t01
        |left join
        |(
        |	SELECT  DISTINCT maingroup_id
        |					,maingroup_text_asc
        |	FROM    cpy_fh01tq68
        |	WHERE   MAINGROUP_LANG = 'zh'
        |) dc2
        |on      t01.supergroup_id = dc2.maingroup_id
        |left join
        |(
        |	SELECT  DISTINCT type_id
        |					,type_text_ascii
        |	FROM    cpy_fh01tq09
        |	WHERE   TYPE_LANG = 'zh'
        |) dc3
        |on      t01.type_id = dc3.type_id
        |left join
        |(
        |	SELECT  DISTINCT loc_id
        |					,loc_text_ascii
        |	FROM    cpy_fh01tq07
        |	WHERE   LOC_LANG = 'zh'
        |) dc4
        |on t01.loc_id = dc4.loc_id
        |left join
        |(
        |	SELECT  DISTINCT locpos_id
        |					,locpos_text_ascii
        |	FROM    cpy_fh01tqe8
        |	WHERE   LOCPOS_LANG = 'zh'
        |) dc5
        |on      t01.locpos_id = dc5.locpos_id
        |LEFT join cpy_fh01tq11 t4
        |on      t01.pos_id = t4.pos_id
        |union all
        |select
        |	t01.spj,
        |	t01.kanr,
        |	t01.defect_ts,
        |	t01.defectstate,
        |	t01.defectclose_ts,
        |	t01.coordinate_x,
        |	t01.coordinate_y,
        |	t01.geraetename_nio,
        |	t01.geraetename_io,
        |	t01.user_id_nio,
        |	t01.user_id_io,
        |	t01.eqs_reading_open,
        |	t01.eqs_reading_close,
        |	t01.capturetime_nio,
        |	t01.capturetime_io,
        |	dc2.maingroup_text_asc,
        |	dc3.type_text_ascii,
        |	dc4.loc_text_ascii,
        |	t4.pos_text_ascii,
        |	dc5.locpos_text_ascii,
        |	t01.repair_ts,
        |	t01.repair_nio_ts,
        |	t01.geraetename_repair,
        |	t01.geraetename_repair_nio,
        |	t01.user_id_repair,
        |	t01.user_id_repair_nio,
        |	from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss') etl_date,
        |	t01.werk,
        |	substr(t01.defect_ts,1,10) cal_date
        |from
        |(
        |	select * from meb_fh04ta01
        |	where defect_ts >= trunc(add_months(from_unixtime(unix_timestamp('${bizdate}', 'yyyyMMdd'), 'yyyy-MM-dd'), -(1)), 'MM')
        | and defect_ts < from_unixtime(unix_timestamp(now()),'yyyy-MM-dd HH:mm:ss')
        |) t01
        |left join
        |(
        |	SELECT  DISTINCT maingroup_id
        |					,maingroup_text_asc
        |	FROM    meb_fh01tq68
        |	WHERE   MAINGROUP_LANG = 'zh'
        |) dc2
        |on      t01.supergroup_id = dc2.maingroup_id
        |left join
        |(
        |	SELECT  DISTINCT type_id
        |					,type_text_ascii
        |	FROM    meb_fh01tq09
        |	WHERE   TYPE_LANG = 'zh'
        |) dc3
        |on      t01.type_id = dc3.type_id
        |left join
        |(
        |	SELECT  DISTINCT loc_id
        |					,loc_text_ascii
        |	FROM    meb_fh01tq07
        |	WHERE   LOC_LANG = 'zh'
        |) dc4
        |on t01.loc_id = dc4.loc_id
        |left join
        |(
        |	SELECT  DISTINCT locpos_id
        |					,locpos_text_ascii
        |	FROM    meb_fh01tqe8
        |	WHERE   LOCPOS_LANG = 'zh'
        |) dc5
        |on      t01.locpos_id = dc5.locpos_id
        |LEFT join meb_fh01tq11 t4
        |on      t01.pos_id = t4.pos_id
        |"""
        .stripMargin)

    sparkKudu.stop()
  }
}
