package com.csvw.mcp

import org.apache.spark.sql.{DataFrame, SparkSession}

object mcp_fab_veh_fh01t04_hi {
  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder().getOrCreate()

    // 替换为实际 Kudu Master 地址
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    val tablelist_fh01t04 = Map(
      "cpy_fh01t04" -> "ods.fab_fis_90068_rpt_cpy_fh01t04_nt_streaming",
      "meb_fh01t04" -> "ods.fab_fis_90103_rpt_meb_fh01t04_nt_streaming",
      "meb_fh01t04" -> "ods.fab_fis_90114_rpt_cpn_fh01t04_nt_streaming",
      "cph_fh01t04" -> "ods.fab_fis_90152_rpt_cph_fh01t04_nt_streaming",
      "cpc_fh01t04" -> "ods.fab_fis_90175_rpt_cpc_fh01t04_nt_streaming"
    )
    // TO4表增加过滤条件
    val filter_t04 = "substr(knr1,3,1) != '9' " +
      "and status0 IN ('A700','R700','L000','L800','M7X0','R100','L100','L500','M100','R500'," +
      "'V900','Z700','Z900','Z897','Z898','Z89X','M795','M800','M810','Q800','Q801','Z800','R480','L480','Q710','Q705')"

    // 动态注册所有表
    tablelist_fh01t04.map { case (targetTable, sourceTable) =>
      val df = sparkKudu.read
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", sourceTable)
        .load()
        .filter(filter_t04)

      df.createOrReplaceTempView(targetTable)
      println(s"已注册表: $targetTable (来源: $sourceTable)")

      // 返回表名和DataFrame的映射
      (targetTable, df)
    }

    val tablelist_fis = Map(
      "cpy_fh01t01" -> "ods.fab_fis_90066_rpt_cpy_fh01t01_nt_streaming",
      "meb_fh01t01" -> "ods.fab_fis_90102_rpt_meb_fh01t01_nt_streaming",
      "cpn_fh01t01" -> "ods.fab_fis_90112_rpt_cpn_fh01t01_nt_streaming",
      "cph_fh01t01" -> "ods.fab_fis_90150_rpt_cph_fh01t01_nt_streaming",
      "cpc_fh01t01" -> "ods.fab_fis_90173_rpt_cpc_fh01t01_nt_streaming",
      "meb_fh01t05" -> "ods.fab_fis_90104_rpt_meb_fh01t05_nt_streaming",
      "cpn_fh01t05" -> "ods.fab_fis_90115_rpt_cpn_fh01t05_nt_streaming"
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
      """
        |insert overwrite table mcp.mcp_fab_veh_fh01t04_hi
        |select
        |	vin,
        |	werk,
        |	spj,
        |	kanr,
        |	plant,                                              -- 工厂名称(日产量报表) CP开头
        |	pr_nr,                                              -- PR号,区分SKD车辆
        |	status0,                                            -- 原始检查点状态
        |	z89x_flag,                                          -- 是否Z89x：是为1,否为0。注释：过Z900同时过Z897和Z898的车辆
        |	is_zp8,                                             -- 是否ZP8：是为1,否为0
        |	is_skd,                                             -- 是否SKD：是为1,否为0
        |	status0_s,                                          -- 车间简称：A、B、P
        |	status0_t,                                          -- 检查点MCP名称
        |	time_slice,                                         -- 2分钟时间分割线
        |	mdatumzeit,                                         -- 车辆过点日期时间
        |	mdatum,                                             -- 车辆过点日期
        |	mzeit,                                              -- 车辆过点时间
        |	vzgi,                                               -- 订单类型: 766和743
        |	modell,                                             -- 车型六位码
        |	series_name,                                        -- 车型名称
        |	brand,                                              -- 车型品牌
        |	anlbgr3,                                            -- 区分CPA3 A车间产线：IM11/IM12
        |	geraetename3,                                       -- 区分CPA3 B车间产线：IR51/IR52
        |	farbau,                                             -- 车身外色
        |	farbin,                                             -- 内饰颜色
        |	knr,                                                -- T01订单号
        |	factory,   											-- 工厂名称(物流报表) PF开头
        |	cal_date,                         				    -- 工厂日期
        |	start_time,                                         -- 工厂每天开始时间
        |	end_time,                                           -- 工厂每天结束时间
        |	row_number() over(partition by werk,spj,kanr,status0_t,cal_date order by cal_date, time_slice) rn,
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |from
        |(
        |	select distinct
        |		p1.vin,
        |		p1.werk,
        |		p1.spj,
        |		p1.kanr,
        |		p1.plant,                                              -- 工厂名称(日产量报表) CP开头
        |		p1.pr_nr,                                              -- PR号,区分SKD车辆
        |		p1.status0,                                            -- 原始检查点状态
        |		p1.z89x_flag,                                          -- 是否Z89x：是为1,否为0。注释：过Z900同时过Z897和Z898的车辆
        |		p1.is_zp8,                                             -- 是否ZP8：是为1,否为0
        |		p1.is_skd,                                             -- 是否SKD：是为1,否为0
        |		p1.status0_s,                                          -- 车间简称：A、B、P
        |		p1.status0_t,                                          -- 检查点MCP名称
        |		p1.time_slice,                                         -- 2分钟时间分割线
        |		p1.mdatumzeit,                                         -- 车辆过点日期时间
        |		p1.mdatum,                                             -- 车辆过点日期
        |		p1.mzeit,                                              -- 车辆过点时间
        |		p1.vzgi,                                               -- 订单类型: 766和743
        |		p1.modell,                                             -- 车型六位码
        |		p1.series_name,                                        -- 车型名称
        |		p1.brand,                                              -- 车型品牌
        |		p1.anlbgr3,                                            -- 区分CPA3 A车间产线：IM11/IM12
        |		p1.geraetename3,                                       -- 区分CPA3 B车间产线：IR51/IR52
        |		p1.farbau,                                             -- 车身外色
        |		p1.farbin,                                             -- 内饰颜色
        |		p1.knr,                                                -- T01订单号
        |		-- CPA3分L1 L2产线
        |		case
        |			-- CPA3在R100、R500(ZP5) B车间车身区分L1和L2产线
        |			when p1.status0 in ('R100','R500') and p1.plant = 'CPA3' then p1.factory
        |			-- CPA3在M01 A车间总装之后分L1和L2产线
        |			when p1.status0_t in ('M01','ZP7','Z89X','ZP8','V900') and p1.plant = 'CPA3' then p2.factory
        |			else replace(p1.plant,'CP','PF')
        |		end factory,  				 						   -- 工厂名称(物流报表) PF开头
        |		p1.cal_date,                         -- 工厂日期
        |	CASE p1.plant
        |					WHEN 'CPH1' THEN '06:00'
        |					WHEN 'CPH2' THEN '06:00'
        |					WHEN 'CPY' THEN '05:00'
        |					WHEN 'CPM' THEN '05:00'
        |					WHEN 'CPA2' THEN '05:00'
        |					WHEN 'CPA3' THEN '05:00'
        |					WHEN 'CPC' THEN '05:30'
        |					WHEN 'CPN' THEN '06:30'
        |		END AS start_time,
        |	CASE p1.plant
        |					WHEN 'CPH1' THEN '06:00'
        |					WHEN 'CPH2' THEN '06:00'
        |					WHEN 'CPY' THEN '05:00'
        |					WHEN 'CPM' THEN '05:00'
        |					WHEN 'CPA2' THEN '05:00'
        |					WHEN 'CPA3' THEN '05:00'
        |					WHEN 'CPC' THEN '05:30'
        |					WHEN 'CPN' THEN '06:30'
        |		END AS end_time,
        |		from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date
        |	from
        |	(
        |		SELECT
        |			a.werk
        |			,a.spj
        |			,a.kanr
        |			,CASE
        |				WHEN a.werk = 'CS' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE3','SV6')) THEN 'CPC'
        |				WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) = 'H' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE2','SV3')) THEN 'CPH1'
        |				WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) IN ('J','K') THEN 'CPH2'
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE3' THEN 'CPY'
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum < '2025-01-01' and d.pr_nr = 'SE1' THEN 'CPA2'
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum >= '2025-01-01' and d.pr_nr = 'SE1' THEN 'CPA3'
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and a.mdatum >= '2025-01-01' and d.pr_nr = 'SE2' THEN 'CPH1'
        |				WHEN a.werk = 'C2' THEN 'CPN'
        |				WHEN a.werk = 'C5' AND a.werk0 = 'C5' THEN 'CPY'
        |				WHEN a.werk = '78' AND a.fanlage2 like '%CP2%' THEN 'CPA2'
        |				WHEN a.werk = '78' AND a.fanlage2 like '%CP3%' THEN 'CPA3'
        |				WHEN a.werk = '78' AND fanlage2 like '%MEB%' THEN 'CPM'
        |			END AS plant
        |			,case
        |				-- CPA3在R100、R500(ZP5) B车间车身区分L1和L2产线
        |				when a.status0 = 'R100' and a.werk = '78' AND a.fanlage2 like '%CP3%' and a.anlbgr3 = 'IR11' then 'PFA3 L1'
        |				when a.status0 = 'R100' and a.werk = '78' AND a.fanlage2 like '%CP3%' and a.anlbgr3 = 'IR12' then 'PFA3 L2'
        |				when a.status0 = 'R500' and a.werk = '78' AND a.fanlage2 like '%CP3%' and a.anlbgr3 = 'IR52' then 'PFA3 L1'
        |				when a.status0 = 'R500' and a.werk = '78' AND a.fanlage2 like '%CP3%' and a.anlbgr3 = 'IR51' then 'PFA3 L2'
        |			end factory
        |			,d.pr_nr
        |			,a.status0
        |			,a.z89x_flag
        |			,case when a.z89x_flag = 1 and a.status0_t in ('ZP8','Z89X') then 1 else 0 end is_zp8
        |			,case when d.pr_nr is not null and a.status0 in ('Z900','V900') then 1 else 0 end is_skd
        |			,a.status0_s
        |			,a.status0_t
        |			,a.time_slice
        |			,a.mdatumzeit
        |			,a.mdatum
        |			,a.mzeit
        |			,CASE
        |				WHEN a.werk = 'CS' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE3','SV6')) THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')- 330 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) = 'H' or (a.werk='78' and a.status0 in ('Z900','V900') and d.pr_nr in ('SE2','SV3')) THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = 'C6' AND substr(a.anlbgr3,-1,1) IN ('J','K') THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN (a.werk = 'C5' AND werk0 = 'C5') OR (a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE3') THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE1' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = 'C2' AND status0 in ('Z900','V900') and d.pr_nr = 'SE2' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-6 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = 'C2' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-390 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = '78' AND a.fanlage2 like '%CP2%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = '78' AND a.fanlage2 like '%CP3%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |				WHEN a.werk = '78' AND fanlage2 like '%MEB%' THEN from_unixtime(unix_timestamp(a.mdatumzeit, 'yyyy-MM-dd HH:mm:ss')-5 * 60 * 60, 'yyyy-MM-dd')
        |			END AS cal_date
        |			,b.vin
        |			,b.vzgi
        |			,b.modell
        |			,COALESCE (c.modell,'OTHERS') series_name
        |			,c.brand
        |			,a.anlbgr3
        |			,a.geraetename3
        |			,b.farbau
        |			,b.farbin
        |			,b.knr
        |		FROM
        |		(
        |		--通过T04表获取工厂检查点数据
        |			select
        |				t1.status0
        |				,t1.werk0
        |				,t1.knr1
        |				,case
        |					when t1.status0 IN ('V900','Z700','Z900','M000','M100','Z897','Z898','Z89X') then 'A'
        |					WHEN t1.status0 IN ('R500','R100') then 'B'
        |					WHEN t1.status0 IN ('L100','L500','L800') then 'P'
        |				END AS status0_s
        |				,CASE t1.status0
        |					WHEN 'R500' THEN 'ZP5'
        |					WHEN 'L500' THEN 'ZP5A'
        |					WHEN 'M100' THEN 'M01'
        |					WHEN 'Z700' THEN 'ZP7'
        |					WHEN 'Z897' THEN 'Z89X'
        |					WHEN 'Z898' THEN 'Z89X'
        |					WHEN 'Z89X' THEN 'Z89X'
        |					WHEN 'Z900' THEN 'ZP8'
        |					WHEN 'V900' THEN 'V900'
        |				END status0_t
        |				,if(floor(cast(substr(mzeit,4,2) as int)/2 + 1)*2=60,concat(lpad(cast(cast(substr(mzeit,1,2) as int)+1 as string),2,'0'), ':00'),
        |				concat(substr(mzeit,1,3),lpad(cast(floor(cast(substr(mzeit,4,2) as int)/2 + 1)*2 as string),2,'0') )) time_slice
        |				,substr(t1.mdatum, 1, 10) mdatum
        |				,substr(t1.mdatumzeit, 1, 19) mdatumzeit
        |				,mzeit
        |				,t1.werk
        |				,t1.spj
        |				,t1.kanr
        |				,t1.fanlage2
        |				,t1.anlbgr3
        |				,t1.geraetename3
        |				,case when t2.kanr is null then 1 else 0 end z89x_flag
        |			from
        |			(
        |			-- 取所有工厂对应检查点数据
        |				-- 仪征
        |				SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpy_fh01t04
        |				WHERE werk = 'C5'
        |				UNION ALL
        |				-- 安亭
        |				SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM meb_fh01t04
        |				WHERE werk = '78'
        |				UNION ALL
        |				-- 南京
        |				SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpn_fh01t04
        |				WHERE werk = 'C2'
        |				UNION ALL
        |				-- 宁波
        |				SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cph_fh01t04
        |				WHERE werk = 'C6'
        |				UNION ALL
        |				-- 长沙
        |				SELECT knr1,werk,spj,kanr,fanlage2,status0,mdatum,mzeit,mdatumzeit,werk0,anlbgr3,geraetename3 FROM cpc_fh01t04
        |				WHERE werk = 'CS'
        |			)t1
        |			left join
        |			(
        |			--过滤Z900中Z89X('Z897','Z898')的数据
        |				select werk,spj,kanr from meb_fh01t04
        |				where substr(knr1,3,1) != '9' AND status0 IN ('Z897','Z898','Z89X') and werk='78'
        |				union all
        |				select werk,spj,kanr from cph_fh01t04
        |				where substr(knr1,3,1) != '9' AND status0 IN ('Z897','Z898','Z89X') and werk='C6'
        |				union all
        |				select werk,spj,kanr from cpy_fh01t04
        |				where substr(knr1,3,1) != '9' AND status0 IN ('Z897','Z898','Z89X') and werk='C5'
        |				union all
        |				select werk,spj,kanr from cpn_fh01t04
        |				where substr(knr1,3,1) != '9' AND status0 IN ('Z897','Z898','Z89X') and werk='C2'
        |				union all
        |				select werk,spj,kanr from cpc_fh01t04
        |				where substr(knr1,3,1) != '9' AND status0 IN ('Z897','Z898','Z89X') and werk='CS'
        |			)t2 on t1.werk= t2.werk and t1.spj = t2.spj and t1.kanr = t2.kanr and t1.status0 = 'Z900'
        |		)a
        |		left join
        |		(
        |		--通过T01表获取车型6位码
        |			select knr, werk, spj, modell, vzgi, farbau, farbin, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpy_fh01t01
        |			union all
        |			select knr, werk, spj, modell, vzgi, farbau, farbin, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from meb_fh01t01
        |			union all
        |			select knr, werk, spj, modell, vzgi, farbau, farbin, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpn_fh01t01
        |			union all
        |			select knr, werk, spj, modell, vzgi, farbau, farbin, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cph_fh01t01
        |			union all
        |			select knr, werk, spj, modell, vzgi, farbau, farbin, CONCAT(FGSTWELT, FGSTSPEZ, FGSTTM, FGSTPZ, FGSTMJ, FGSTWK, FGSTLFD) AS VIN from cpc_fh01t01
        |		)b on a.werk = b.werk and a.spj = b.spj and a.knr1 = b.knr
        |		--通过车型6位码获取车系名称
        |		left join
        |		(
        |		-- 防止手工数据出现一个六位码对应两个以上车型名称, 随机选择一个。
        |			select
        |				modell,
        |				brand,
        |				code_6,
        |				row_number() over (partition by code_6 order by modell) rn
        |			from analytical_db_manual_table.mcp_pf_model_six_code_df
        |		)c
        |		on trim(b.modell) = c.code_6 and c.rn = 1
        |		-- 获取SKD车子ZP8点车辆
        |		left join
        |		(
        |			-- 安亭CPM和CPA3 SKD车辆转往两个厂区：CPC(SE3、SV6)、CPH1(SE2、SV3)
        |			select
        |				spj,
        |				werk,
        |				knr,
        |				CASE
        |					WHEN pnrstring LIKE '%SE2%' THEN 'SE2'
        |					WHEN pnrstring LIKE '%SV3%' THEN 'SV3'
        |					WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |					WHEN pnrstring LIKE '%SV6%' THEN 'SV6'
        |				END pr_nr
        |			from meb_fh01t05
        |			union
        |			-- 南京SKD车辆转往三个厂区：CPA3-L2(SE1)、CPH1(SE2)、CPY(SE3)
        |			-- 2025之前转往两个厂区：CPA2(SE1)、CPY(SE3)
        |			select
        |				spj,
        |				werk,
        |				knr,
        |				CASE
        |					WHEN pnrstring LIKE '%SE1%' THEN 'SE1'
        |					WHEN pnrstring LIKE '%SE2%' THEN 'SE2'
        |					WHEN pnrstring LIKE '%SE3%' THEN 'SE3'
        |				END pr_nr
        |			from fh01t05
        |		)d
        |		on a.werk = d.werk AND a.spj = d.spj AND a.knr1 = d.knr
        |	) p1
        |	left join
        |	(
        |	-- 总装 区分产线
        |		SELECT
        |			werk,
        |			spj,
        |			kanr,
        |			factory,
        |			'A' status0_s,
        |			ROW_NUMBER() OVER(PARTITION BY werk,spj,kanr order by 1 desc) rn
        |		FROM
        |		(
        |		-- 安亭三厂过ZP8(Z900)的车辆 区分一线(anlbgr3=IM11)和二线(anlbgr3=IM12)
        |			SELECT
        |				werk,
        |				spj,
        |				kanr,
        |				case
        |					when anlbgr3 = 'IM11' then 'PFA3 L1'
        |					when anlbgr3 = 'IM12' then 'PFA3 L2'
        |				end factory
        |			FROM meb_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 = 'M100' and werk = '78' And fanlage2 like '%CP3%'
        |			union all
        |			-- 部分南京车辆转到CPA3 L2报交
        |			SELECT
        |				werk,
        |				spj,
        |				kanr,
        |				'PFA3 L2' factory
        |			FROM cpn_fh01t04
        |			WHERE substr(knr1,3,1) != '9' AND status0 = 'M100' and werk = 'C2'
        |		) T
        |	) p2 on p1.werk= p2.werk
        |		and p1.spj = p2.spj
        |		and p1.kanr = p2.kanr
        |		and p1.status0_s = p2.status0_s
        |		and p2.rn = 1
        |) p
        |"""
        .stripMargin)
    println("---------计算完成--------")

    sparkKudu.stop()
  }
}
