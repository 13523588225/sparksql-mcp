package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager
import java.util.Properties

object mcp_fab_daily_volume_10mif_bak {
  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val cpy_fh01t04 = "ods.fab_fis_90068_rpt_cpy_fh01t04_nt_streaming"
    val meb_fh01t04 = "ods.fab_fis_90103_rpt_meb_fh01t04_nt_streaming"
    val cpn_fh01t04 = "ods.fab_fis_90114_rpt_cpn_fh01t04_nt_streaming"
    val cph_fh01t04 = "ods.fab_fis_90152_rpt_cph_fh01t04_nt_streaming"
    val cpc_fh01t04 = "ods.fab_fis_90175_rpt_cpc_fh01t04_nt_streaming"
    val cpy_fh01t01 = "ods.fab_fis_90066_rpt_cpy_fh01t01_nt_streaming"
    val meb_fh01t01 = "ods.fab_fis_90102_rpt_meb_fh01t01_nt_streaming"
    val cpn_fh01t01 = "ods.fab_fis_90112_rpt_cpn_fh01t01_nt_streaming"
    val cph_fh01t01 = "ods.fab_fis_90150_rpt_cph_fh01t01_nt_streaming"
    val cpc_fh01t01 = "ods.fab_fis_90173_rpt_cpc_fh01t01_nt_streaming"
    val meb_fh01t05 = "ods.fab_fis_90104_rpt_meb_fh01t05_nt_streaming"
    val cpn_fh01t05 = "ods.fab_fis_90115_rpt_cpn_fh01t05_nt_streaming"

    // 读取 Kudu 表数据
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpy_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("cpy_fh01t04")

    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("meb_fh01t04")

    val kuduDF3: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpn_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF3.createOrReplaceTempView("cpn_fh01t04")

    val kuduDF4: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cph_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF4.createOrReplaceTempView("cph_fh01t04")

    val kuduDF5: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF5.createOrReplaceTempView("cpc_fh01t04")

    val kuduDF6: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpy_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF6.createOrReplaceTempView("cpy_fh01t01")

    val kuduDF7: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF7.createOrReplaceTempView("meb_fh01t01")

    val kuduDF8: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpn_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF8.createOrReplaceTempView("cpn_fh01t01")

    val kuduDF9: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cph_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF9.createOrReplaceTempView("cph_fh01t01")

    val kuduDF10: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpc_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF10.createOrReplaceTempView("cpc_fh01t01")

    val kuduDF11: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t05)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF11.createOrReplaceTempView("meb_fh01t05")

    val kuduDF12: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", cpn_fh01t05)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF12.createOrReplaceTempView("cpn_fh01t05")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("SparkSQL Kudu Hive Opration Demo")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    val sourceDF: DataFrame = sparkHive.sql(
      """
        |select distinct
        |	p1.werk,
        |	p1.spj,
        |	p1.kanr,
        |	-- CPA3分L1 L2产线
        |	case
        |		-- CPA3在M01,ZP7 A车间总装之后分L1和L2产线
        |		when p1.status0 in ('M100','Z700') and p1.plant = 'CPA3' then replace(p2.factory,'PF','CP')
        |		else p1.plant
        |	end plant,  				 						   -- 工厂名称(日产量报表) CP开头
        |	p1.status0_t,
        |	p1.time_slice,                                         -- 2分钟时间分割线
        |	p1.mdatum,                                             -- 车辆过点日期
        |	p1.mzeit,                                              -- 车辆过点时间
        |	p1.cal_date,                         				   -- 工厂日期
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	SELECT
        |		a.werk
        |		,a.spj
        |		,a.kanr
        |		,CASE
        |			WHEN a.werk = 'CS' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE3','SV6')) THEN 'CPC'
        |			WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) = 'H' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE2','SV3')) THEN 'CPH1'
        |			WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) IN ('J','K') THEN 'CPH2'
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE3' THEN 'CPY'
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum < '2025-01-01' and d.pr_nr = 'SE1' THEN 'CPA2'
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum >= '2025-01-01' and d.pr_nr = 'SE1' THEN 'CPA3'
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum >= '2025-01-01' and d.pr_nr = 'SE2' THEN 'CPH1'
        |			WHEN a.werk = 'C2' THEN 'CPN'
        |			WHEN a.werk = 'C5' AND a.werk0 = 'C5' THEN 'CPY'
        |			WHEN a.werk = '78' AND a.fanlage2 like '%CP2%' THEN 'CPA2'
        |			WHEN a.werk = '78' AND a.fanlage2 like '%CP3%' THEN 'CPA3'
        |			WHEN a.werk = '78' AND fanlage2 like '%MEB%' THEN 'CPM'
        |		END AS plant
        |		,d.pr_nr
        |		,a.status0
        |		,a.status0_s
        |		,CASE
        |			WHEN a.status0 = 'M100' THEN 'M01'
        |			WHEN a.status0 = 'R100' THEN 'R1'
        |			WHEN a.status0 = 'R500' THEN 'R8'
        |			WHEN a.status0 = 'Z700' and COALESCE (b.vzgi,'OTHERS') != '743' THEN 'Z7'
        |			WHEN a.status0 = 'Z900' and COALESCE (b.vzgi,'OTHERS') != '743' THEN 'Z8'
        |			WHEN a.status0 = 'L100' THEN 'L1'
        |			WHEN a.status0 = 'L500' THEN 'L5'
        |			WHEN a.status0 = 'V900' THEN 'V900'
        |		END AS status0_t
        |		,a.time_slice
        |		,a.mdatumzeit
        |		,a.mdatum
        |		,a.mzeit
        |		,CASE
        |			WHEN a.werk = 'CS' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE3','SV6')) THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')- 330 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) = 'H' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE2','SV3')) THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) IN ('J','K') THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN (a.werk = 'C5' AND werk0 = 'C5') OR (a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE3') THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE1' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE2' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = 'C2' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-390 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = '78' AND a.fanlage2 like '%CP2%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = '78' AND a.fanlage2 like '%CP3%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |			WHEN a.werk = '78' AND fanlage2 like '%MEB%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |		END AS cal_date
        |		,b.vzgi
        |	FROM
        |	(
        |	--通过T04表获取工厂检查点数据
        |		select
        |			t1.status0
        |			,t1.werk0
        |			,t1.knr1
        |			,case
        |				when t1.status0 IN ('V900','Z700','Z900','M000','M100') then 'A'
        |				WHEN t1.status0 IN ('R500','R100') then 'B'
        |				WHEN t1.status0 IN ('L100','L500','L800') then 'P'
        |			END AS status0_s
        |			,CASE t1.status0
        |				WHEN 'R500' THEN 'ZP5'
        |				WHEN 'L500' THEN 'ZP5A'
        |				WHEN 'M100' THEN 'M01'
        |				WHEN 'Z700' THEN 'ZP7'
        |				WHEN 'Z897' THEN 'Z89X'
        |				WHEN 'Z898' THEN 'Z89X'
        |				WHEN 'Z900' THEN 'ZP8'
        |				WHEN 'V900' THEN 'V900'
        |			END status0_t
        |			,if(floor(cast(substr(mzeit,4,2) as int)/2 + 1)*2=60,concat(lpad(cast(cast(substr(mzeit,1,2) as int)+1 as string),2,'0'), ':00'),
        |			concat(substr(mzeit,1,3),lpad(cast(floor(cast(substr(mzeit,4,2) as int)/2 + 1)*2 as string),2,'0') )) time_slice
        |			,substr(t1.mdatum, 1, 10) mdatum
        |			,substr(t1.mdatumzeit, 1, 19) mdatumzeit
        |			,mzeit
        |			,t1.werk
        |			,t1.spj
        |			,t1.kanr
        |			,t1.fanlage2
        |			,t1.anlbgr3
        |		from
        |		(
        |		-- 取所有工厂对应检查点数据
        |			-- 仪征
        |			SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpy_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 IN ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898') and werk = 'C5'
        |			and mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |			UNION ALL
        |			-- 安亭
        |			SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM meb_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 IN ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898') and werk = '78'
        |			and mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |			UNION ALL
        |			-- 南京
        |			SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpn_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 IN ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898') and werk = 'C2'
        |			and mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |			UNION ALL
        |			-- 宁波
        |			SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cph_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 IN ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898') and werk = 'C6'
        |			and mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |			UNION ALL
        |			-- 长沙
        |			SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpc_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 IN ('M100','R100','R500','Z700','Z900','L100','L500','V900','Z897','Z898') and werk = 'CS'
        |			and mdatumzeit >= from_unixtime(unix_timestamp() - 24 * 60 * 60, 'yyyy-MM-dd HH:mm:ss')
        |		)t1
        |		-- 剔除Z89X的Z900点车辆
        |		left join mcp.mcp_fab_veh_fh01t04_z89x_df t2
        |		on t1.werk= t2.werk and t1.spj = t2.spj and t1.kanr = t2.kanr and t1.status0 = 'Z900'
        |		where t2.kanr is null
        |	)a
        |	left join
        |	(
        |	--通过T01表获取车型6位码
        |		select werk, spj, kanr0, vzgi from cpy_fh01t01
        |		union all
        |		select werk, spj, kanr0, vzgi from meb_fh01t01
        |		union all
        |		select werk, spj, kanr0, vzgi from cpn_fh01t01
        |		union all
        |		select werk, spj, kanr0, vzgi from cph_fh01t01
        |		union all
        |		select werk, spj, kanr0, vzgi from cpc_fh01t01
        |	)b on a.werk = b.werk and a.spj = b.spj and a.kanr = b.kanr0
        |	-- 获取SKD车子ZP8点车辆
        |	left join
        |	(
        |		select
        |			spj,
        |			werk,
        |			knr,
        |			CASE
        |				WHEN pnrstring LIKE '%SE2%' THEN 'SE2'
        |				WHEN pnrstring LIKE '%SV3%' THEN 'SV3'
        |				WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |				WHEN pnrstring LIKE '%SV6%' THEN 'SV6'
        |			END pr_nr
        |		from meb_fh01t05
        |		union
        |		select
        |			spj,
        |			werk,
        |			knr,
        |			CASE
        |				WHEN pnrstring LIKE '%SE1%' THEN 'SE1'
        |				WHEN pnrstring LIKE '%SE2%' THEN 'SE2'
        |				WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |			END pr_nr
        |		from cpn_fh01t05
        |	)d
        |	on a.werk = d.werk AND a.spj = d.spj AND a.knr1 = d.knr
        |) p1
        |left join
        |(
        |-- 总装 区分产线
        |	SELECT
        |		werk,
        |		spj,
        |		kanr,
        |		factory,
        |		'A' status0_s,
        |		ROW_NUMBER() OVER(PARTITION BY werk,spj,kanr order by 1 desc) rn
        |	FROM
        |	(
        |	-- 安亭三厂过ZP8(Z900)的车辆 区分一线(anlbgr3=IM11)和二线(anlbgr3=IM12)
        |		SELECT
        |			werk,
        |			spj,
        |			kanr,
        |			case
        |				when anlbgr3 = 'IM11' then 'PFA3 L1'
        |				when anlbgr3 = 'IM12' then 'PFA3 L2'
        |			end factory
        |		FROM meb_fh01t04
        |		WHERE substr(knr1,3,1) != '9' AND status0 = 'M100' and werk = '78' And fanlage2 like '%CP3%'
        |		union all
        |		-- 部分南京车辆转到CPA3 L2报交
        |		SELECT
        |			werk,
        |			spj,
        |			kanr,
        |			'PFA3 L2' factory
        |		FROM cpn_fh01t04
        |		WHERE substr(knr1,3,1) != '9' AND status0 = 'M100' and werk = 'C2'
        |	) T
        |) p2 on p1.werk= p2.werk
        |	and p1.spj = p2.spj
        |	and p1.kanr = p2.kanr
        |	and p1.status0_s = p2.status0_s
        |	and p2.rn = 1
        |where p1.plant is not null
        |"""
        .stripMargin)

    //获取mysql配置文件
    val inputStream = getClass.getClassLoader.getResourceAsStream("mysql.properties")
    val props = new Properties
    // 加载配置文件
    props.load(inputStream)
    // 从配置文件中获取数据库连接信息
    val url = props.getProperty("url")
    //数据插入mysql表
    val table = "mcp_fab_daily_volume_2mif"
    sourceDF.write.mode("overwrite").jdbc(url, table, props)

    // 创建数据库连接
    val connection = DriverManager.getConnection(url, props)

    // 调用存储过程
    val callableStatement = connection.prepareCall("{call app.mcp_fab_daily_volume_2mif_procedure()}")
    callableStatement.execute()

    inputStream.close()
    sparkKudu.stop()
    sparkHive.stop()
  }
}
