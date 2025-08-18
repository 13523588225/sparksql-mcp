package com.csvw.mcp

import org.apache.spark.sql.SparkSession

object mcp_fab_veh_dlq_detail_hi {
  def main(args: Array[String]): Unit = {

    // 开始日期
    val bizdate = args(0)
    println(s"开始日期: $bizdate")

    val sparkHive: SparkSession = SparkSession.builder()
      .appName("mcp_fab_veh_dlq_detail_hi")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 输出到hive
    sparkHive.sql(
      s"""
        |insert overwrite table mcp.mcp_fab_veh_dlq_detail_hi partition (plant_date)
        |select
        |	plant,
        |	checkpoint,
        |	capture_time,
        |	if(floor(cast(substr(plant_time,15,2) as int)/2 + 1)*2=60,
        |		concat(lpad(cast(cast(substr(plant_time,12,2) as int)+1 as string),2,'0'), ':00'),
        |		concat(substr(plant_time,12,3),lpad(cast(floor(cast(substr(plant_time,15,2) as int)/2 + 1)*2 as string),2,'0') )
        |	) AS plant_time_slice,
        |	series_code_6,
        |	series_name_6,
        |	werk,
        |	spj,
        |	kanr,
        |	BHG,
        |	HG,
        |	ZS,
        |	torque_BHG,
        |	torque_HG,
        |	torque_ZS,
        |	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') etl_date,
        |	plant_date
        |from
        |(
        |	select
        |		plant,
        |		checkpoint,
        |		capture_time,
        |		from_unixtime(unix_timestamp(substr(capture_time,1,19),'yyyy-MM-dd HH:mm:ss') - p2.mi * 60, 'yyyy-MM-dd HH:mm:ss') plant_time,
        |		series_code_6,
        |		series_name_6,
        |		werk,
        |		spj,
        |		kanr,
        |		NVL(BHG,0) BHG,
        |		NVL(HG,0) HG,
        |		NVL(ZS,0) ZS,
        |		torque_BHG,
        |		torque_HG,
        |		torque_ZS,
        |		plant_date
        |	from
        |	(
        |		select
        |			plant,
        |			capture_time,
        |			series_code_6,
        |			series_name_6,
        |			werk,
        |			spj,
        |			kanr,
        |			NVL(BHG,0) BHG,
        |			NVL(HG,0) HG,
        |			NVL(ZS,0) ZS,
        |			null torque_BHG,
        |			null torque_HG,
        |			null torque_ZS,
        |			plant_date,
        |			checkpoint
        |		from
        |		(
        |			-- Water & Road
        |			-- CPM CPH CPC CPY
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				NVL(BHG,0) + NVL(HG,0) as ZS,
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date, checkpoint order by capture_time) rn
        |			from
        |			(
        |				select
        |					plant,
        |					plant_date,
        |					capture_time,
        |					-- 雨淋: Water   路试: Road
        |					case
        |						when plant = 'CPM' AND check_name = 'E4MPAQ25AROA0000XXXX' then 'Road'
        |						when plant = 'CPM' AND check_name = 'E4MPAQ26ARAI0000XXXX' then 'Water'
        |						when plant = 'CPH2' AND check_name = 'P2AQ251R060000' and check_value_id in ('2','4') then 'Road'
        |						when plant = 'CPH2' AND check_name = 'P2AQ261R070000' and check_value_id in ('2','4') then 'Water'
        |						when plant = 'CPC' AND check_name = 'ALCPAQ26B0260000XXXX' and check_value_id in ('2','4') then 'Road'
        |						when plant = 'CPC' AND check_name = 'ALCPAQ27B0270000XXXX' then 'Water'
        |						when plant = 'CPY' AND check_name = 'ALCPAQ27B0270000XXXX' then 'Road'
        |						when plant = 'CPY' AND check_name = 'ALCPAQ28B0280000XXXX' then 'Water'
        |					end as checkpoint,
        |					modell series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					case when check_value_id = '4' then 1 end BHG,
        |					case when check_value_id <> '4' then 1 end HG
        |				from mcp.mcp_fab_veh_fh04ta06_check_hi
        |				where plant IN ('CPM','CPH2','CPC','CPY')
        |			) t
        |			where checkpoint is not null
        |			union all
        |			-- VP1 & VP2
        |			-- CPA2 CPA3 CPM CPY CPH1 CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				NVL(BHG,0) + NVL(HG,0) as ZS,
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date, checkpoint order by capture_time) rn
        |			from
        |			(
        |				SELECT
        |					plant,
        |					plant_date,
        |					capture_time,
        |					case
        |						when msr_value_id IN ('110','150') AND plant ='CPM' then 'VP1'
        |						when msr_value_id = '110' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') then 'VP1'
        |						when msr_value_id IN ('510','520') AND plant ='CPM' then 'VP2'
        |						when msr_value_id = '510' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') then 'VP2'
        |					END CHECKPOINT,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					case
        |						when msr_value_id IN ('110','150') AND plant ='CPM' and value = 'dlq' then 1
        |						when msr_value_id = '110' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') and value = 'dlq' then 1
        |						when msr_value_id IN ('510','520') AND plant ='CPM' and value = 'dlq' then 1
        |						when msr_value_id = '510' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') and value = 'dlq' then 1
        |					end BHG,
        |					case
        |						when msr_value_id IN ('110','150') AND plant ='CPM' and value = 'dlq' then 1
        |						when msr_value_id = '110' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') and value = 'DLQ' then 1
        |						when msr_value_id IN ('510','520') AND plant ='CPM' and value = 'dlq' then 1
        |						when msr_value_id = '510' AND plant in ('CPA2','CPA3','CPY','CPH1','CPH2' ,'CPC') and value = 'DLQ' then 1
        |					end HG
        |				FROM mcp.mcp_fab_veh_result_info_hi
        |				WHERE value IN ('DLQ','dlq')
        |				and result_name = '000800'
        |				and msr_value_id in ('110','150','510','520')
        |				and plant is not null
        |			) t
        |			where checkpoint is not null
        |			union all
        |			-- VP3
        |			-- CPA2 CPA3 CPM CPY CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'VP3' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				ZS,
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by capture_time) rn
        |			from
        |			(
        |				--1. 获取 CPA3 CPY CPH2 CPC
        |				select
        |					plant,
        |					capture_time,
        |					plant_date,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					CASE WHEN value = 'DLQ' THEN 1 END HG,
        |					CASE WHEN value = 'dlq' THEN 1 END BHG,
        |					1 ZS
        |				from mcp.mcp_fab_veh_result_info_hi
        |				WHERE value IN ('DLQ','dlq')
        |				and result_name = '000800'
        |				and msr_value_id = '700'
        |				and plant in ('CPY','CPC','CPA3','CPH2')
        |			) t
        |			union all
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'VP3' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				ZS,
        |				rn
        |			from
        |			(
        |				--2.1 CPA2 CPM 总数
        |				SELECT
        |					plant,
        |					mdatumzeit capture_time,
        |					CAL_DATE plant_date,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					null BHG,
        |					1 ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, CAL_DATE order by mdatumzeit) rn
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |				WHERE plant in ('CPA2','CPM')
        |					AND status0 = 'Z700'
        |				union all
        |				--2.2 CPA2 CPM 不合格
        |				select
        |					plant,
        |					Capture_time,
        |					plant_date,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					1 BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by capture_time) rn
        |				from
        |				(
        |					select
        |						b.plant,
        |						b.plant_date,
        |						b.Capture_time,
        |						b.series_code_6,
        |						b.series_name_6,
        |						b.werk,
        |						b.spj,
        |						b.kanr
        |					from
        |					(
        |						select * from mcp.mcp_fab_veh_fh01t04_hf
        |						where status0 = 'Z700'
        |					)a
        |					join
        |					(
        |						select * from
        |						(
        |							select *,
        |								ROW_NUMBER() over(PARTITION by werk, spj, kanr order by Capture_time desc) rn
        |							from mcp.mcp_fab_veh_result_info_hi
        |							where result_name = '000700'
        |							and plant in ('CPA2','CPM')
        |						) t1
        |						where result_value_id = '13' and rn = 1
        |					)b on a.werk=b.werk and a.spj=b.spj and a.kanr=b.kanr
        |				) ta
        |			) t
        |			union all
        |			-- Filling
        |			-- CPA2 CPA3 CPM CPY CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'Filling' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				nvl(ZS,0) - nvl(BHG,0) as HG,
        |				ZS,
        |				RN
        |			from
        |			(
        |				-- 总数
        |				SELECT
        |					plant,
        |					mdatumzeit capture_time,
        |					CAL_DATE plant_date,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					null BHG,
        |					1 ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, CAL_DATE order by mdatumzeit) rn
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |				WHERE plant is not null and status0 = 'Z700'
        |				union all
        |				-- 不合格
        |				select
        |					plant,
        |					Capture_time,
        |					plant_date,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					1 BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time) rn
        |				from
        |				(
        |					select
        |						b.plant,
        |						b.plant_date,
        |						b.Capture_time,
        |						b.series_code_6,
        |						b.series_name_6,
        |						b.werk,
        |						b.spj,
        |						b.kanr
        |					from
        |					(
        |						select * from mcp.mcp_fab_veh_fh01t04_hf
        |						where status0 = 'Z700'
        |					)a
        |					join
        |					(
        |						select * from
        |						(
        |							select *
        |								,ROW_NUMBER() over (partition by werk,spj,kanr,result_name order by capture_time) rn
        |							from mcp.mcp_fab_veh_result_info_hi
        |							WHERE result_value_id IN ('10','13')
        |								AND geraetename  like '%FL%'
        |						) t1
        |						where result_value_id = '13' and rn = 1
        |					)b on a.werk=b.werk and a.spj=b.spj and a.kanr=b.kanr
        |				) ta
        |			) t
        |			union all
        |			-- ZP8
        |			-- CPA2 CPA3 CPM CPY CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'ZP8' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				nvl(ZS,0) - nvl(BHG,0) as HG,
        |				ZS,
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time) rn
        |			FROM
        |			(
        |			--获取车辆总数
        |				SELECT
        |					plant,
        |					mdatum plant_date,
        |					mdatumzeit capture_time,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					CASE
        |						WHEN plant = 'CPN' AND status0 IN ('M800','M810') THEN 1
        |						WHEN plant = 'CPY' AND status0 IN ('M795','M800') AND GERAETENAME3 IN ( 'HC5ZP815','HC5ZP825','HC5ZP835','HC5ZP845','HC5ZP855','HC5ZP865','HC5ZP875','HC5ZP885','HC5ZP895','HC5ZP8A5','HC5ZP8B5','HC5ZP8C5','HC5ZP8D5','HC5ZP8E5','HC5ZP8F5','HC5ZP8G5','HC5ZP8H5','HC5ZP8I5','HC5ZP8J5','HC5ZP8K5','HC5ZP8L5','QC5ZP815','QC5ZP825','QC5ZP835') THEN 1
        |						WHEN plant = 'CPC' AND status0 IN ('M795','M800') THEN 1
        |						WHEN plant = 'CPA2' AND status0 = 'M795' AND GERAETENAME3 IN ( 'H78ZP816','H78ZP826','H78ZP836','H78ZP846','H78ZP856','H78ZP866','H78ZP876','H78ZP886','H78ZP896','H78ZP8A6','H78ZP8B6','H78ZP8C6','H78ZP8D6','H78ZP8E6','H78ZP8F6','H78ZP8G6','H78ZP8H6','H78ZP8I6','H78ZP8J6','H78ZP8K6','H78ZP8L6','Q78ZP816','Q78ZP826','Q78ZP836'  ) THEN 1
        |						WHEN plant = 'CPA3' AND status0 = 'M795' AND GERAETENAME3 IN ( 'Q78ZP811','Q78ZP821','H78ZP861','H78ZP871','H78ZP881','H78ZP891','H78ZP8A1','H78ZP8B1','H78ZP8C1','H78ZP8D1','H78ZP8E1','H78ZP8F1','H78ZP8G1','H78ZP8H1','H78ZP8I1','H78ZP8J1','H78ZP8K1','H78ZP8L1'  ) THEN 1
        |						WHEN plant = 'CPM' AND status0 = 'M795' AND GERAETENAME3 IN ( 'H78ZP81A','H78ZP82A','H78ZP83A','H78ZP84A','H78ZP85A','H78ZP86A','H78ZP87A','H78ZP88A','H78ZP89A','H78ZP8AA','H78ZP8BA','H78ZP8CA','H78ZP8DA','H78ZP8EA','H78ZP8FA','H78ZP8GA','H78ZP8HA','H78ZP8IA','H78ZP8JA','H78ZP8KA','Q78ZP81A','Q78ZP82A','H78ZRW1A','H78ZRW2A','H78ZRW3A','H78ZRW4A'  )THEN 1
        |						WHEN plant = 'CPH1' AND status0 = 'M800' THEN 1
        |						WHEN plant = 'CPH2' AND status0 = 'M800' THEN 1
        |					END ZS,
        |					null BHG
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |			)a
        |			WHERE ZS = 1
        |			union all
        |			-- ZP8
        |			-- CPA2 CPA3 CPM CPY CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'ZP8' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				null HG,
        |				ZS,
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time) rn
        |			FROM
        |			(
        |				-- 获取不合格车辆
        |				SELECT
        |					plant,
        |					mdatum plant_date,
        |					mdatumzeit capture_time,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null ZS,
        |					CASE
        |						WHEN plant = 'CPN' AND status0 IN ('Z800','Q800') THEN 1
        |						WHEN plant = 'CPY' AND status0 IN ('Q800','Z800') AND GERAETENAME3 IN ( 'HC5ZP815','HC5ZP825','HC5ZP835','HC5ZP845','HC5ZP855','HC5ZP865','HC5ZP875','HC5ZP885','HC5ZP895','HC5ZP8A5','HC5ZP8B5','HC5ZP8C5','HC5ZP8D5','HC5ZP8E5','HC5ZP8F5','HC5ZP8G5','HC5ZP8H5','HC5ZP8I5','HC5ZP8J5','HC5ZP8K5','HC5ZP8L5','QC5ZP815','QC5ZP825','QC5ZP835') THEN 1
        |						WHEN plant = 'CPC' AND status0 IN ('Z800','Q801') and GERAETENAME3 IN ('HCSZP81C','HCSZP82C','HCSZP83C','HCSZP84C','HCSZP85C','HCSZP86C','HCSZP87C','HCSZP88C','HCSZP89C','HCSZP8AC','HCSZP8BC','HCSZP8CC','HCSZP8DC','HCSZP8EC','HCSZP8FC','HCSZP8GC','HCSZP8HC','HCSZP8IC','HCSZP8JC','HCSZP8KC','HCSZP8LC','QCSZP81C','QCSZP82C','QCSZP83C') THEN 1
        |						WHEN plant = 'CPA2' AND status0 IN ('Z800','Q801') AND GERAETENAME3 IN ( 'H78ZP816','H78ZP826','H78ZP836','H78ZP846','H78ZP856','H78ZP866','H78ZP876','H78ZP886','H78ZP896','H78ZP8A6','H78ZP8B6','H78ZP8C6','H78ZP8D6','H78ZP8E6','H78ZP8F6','H78ZP8G6','H78ZP8H6','H78ZP8I6','H78ZP8J6','H78ZP8K6','H78ZP8L6','Q78ZP816','Q78ZP826','Q78ZP836'  ) THEN 1
        |						WHEN plant = 'CPA3' AND status0 IN ('Z800','Q800') AND GERAETENAME3 IN ( 'Q78ZP811','Q78ZP821','H78ZP861','H78ZP871','H78ZP881','H78ZP891','H78ZP8A1','H78ZP8B1','H78ZP8C1','H78ZP8D1','H78ZP8E1','H78ZP8F1','H78ZP8G1','H78ZP8H1','H78ZP8I1','H78ZP8J1','H78ZP8K1','H78ZP8L1'  ) THEN 1
        |						WHEN plant = 'CPM' AND status0 IN ('Z800','Q800') AND GERAETENAME3 IN ( 'H78ZP81A','H78ZP82A','H78ZP83A','H78ZP84A','H78ZP85A','H78ZP86A','H78ZP87A','H78ZP88A','H78ZP89A','H78ZP8AA','H78ZP8BA','H78ZP8CA','H78ZP8DA','H78ZP8EA','H78ZP8FA','H78ZP8GA','H78ZP8HA','H78ZP8IA','H78ZP8JA','H78ZP8KA','Q78ZP81A','Q78ZP82A','H78ZRW1A','H78ZRW2A','H78ZRW3A','H78ZRW4A'  )THEN 1
        |						WHEN plant = 'CPH1' AND status0 IN ('Z800','Q801') THEN 1
        |						WHEN plant = 'CPH2' AND status0 IN ('Z800','Q801') THEN 1
        |					END BHG
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |			)a
        |			WHERE BHG = 1
        |			union all
        |			-- ZP5
        |			-- CPA2 CPA3 CPM CPY CPH1 CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'ZP5' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				nvl(ZS,0) - nvl(BHG,0) as HG,
        |				ZS,
        |				1 rn
        |			FROM
        |			(
        |			--获取车辆明细
        |				SELECT
        |					plant,
        |					cal_date plant_date,
        |					mdatumzeit capture_time,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					status0,
        |					case
        |						when status0 = 'R500' then 1
        |					end ZS,
        |					case
        |						when status0 = 'R480' then 1
        |					end BHG
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |				where plant is not null
        |				and rn = 1
        |				and status0 IN ('R480','R500')
        |			)a
        |			union all
        |			-- ZP5A
        |			-- CPA2 CPA3 CPM CPY CPH1 CPH2 CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'ZP5A' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				nvl(ZS,0) - nvl(BHG,0) as HG,
        |				ZS,
        |				rank() over(PARTITION by werk, spj, kanr, plant_date, status0 order by Capture_time) rn
        |			FROM
        |			(
        |			--获取车辆明细
        |				SELECT
        |					plant,
        |					cal_date plant_date,
        |					mdatumzeit capture_time,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					status0,
        |					case
        |						when status0 = 'L500' then 1
        |					end ZS,
        |					case
        |						when status0 = 'L480' then 1
        |					end BHG
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |				WHERE plant in ('CPM', 'CPA3', 'CPH2', 'CPC', 'CPY')
        |				AND status0 IN ('L500','L480')
        |			)a
        |			union all
        |			-- ALS
        |			-- CPM,CPH2,CPC,CPY
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'ALS' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				NVL(BHG,0) + NVL(HG,0) AS ZS,
        |				rn
        |			from
        |			(
        |				-- 合格
        |				select
        |					b.plant,
        |					b.plant_date,
        |					b.Capture_time,
        |					b.series_code_6,
        |					b.series_name_6,
        |					b.werk,
        |					b.spj,
        |					b.kanr,
        |					1 HG,
        |					null BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by b.werk, b.spj, b.kanr, b.plant_date order by b.Capture_time) rn
        |				from
        |				(
        |					--依据werk,spj,kanr,获取result_value_id全为10的数据
        |					select werk, spj, kanr from mcp.mcp_fab_veh_result_info_hi
        |					where Result_value_id in('10','13')
        |					and (substr(Result_name,-3) in ('258', '259', '701') or substr(Result_name,-4)= '2813')
        |					group by werk, spj, kanr having sum(CAST(Result_value_id AS INT))/count(1)=10
        |				)a
        |				join
        |				(
        |					--依据Result_value_id=10,按照Capture_time 升序进行排序，取top1 最早的时间
        |					select
        |						*,
        |						ROW_NUMBER() over(PARTITION by werk, spj, kanr order by Capture_time asc) rn
        |					from
        |						mcp.mcp_fab_veh_result_info_hi
        |					where
        |						Result_value_id = '10'
        |						and plant in ('CPM','CPH2','CPC','CPY')
        |						and (substr(Result_name,-3) in ('258', '259', '701') or substr(Result_name,-4)= '2813')
        |				)b on a.werk = b.werk
        |					and a.spj = b.spj
        |					and a.kanr = b.kanr
        |					and b.rn = 1
        |				union all
        |				-- 不合格
        |				select
        |					plant,
        |					plant_date,
        |					Capture_time,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					1 BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time) rn
        |				from
        |				(
        |					select
        |						*,
        |						ROW_NUMBER() over(PARTITION by werk, spj, kanr order by Capture_time asc) rn
        |					from mcp.mcp_fab_veh_result_info_hi
        |					where
        |						Result_value_id = '13'
        |						and plant in ('CPM','CPH2','CPC','CPY')
        |						and (substr(Result_name,-3) in ('258', '259', '701') or substr(Result_name,-4)= '2813')
        |				) ta WHERE RN = 1
        |			) t
        |			union all
        |			-- Rolling
        |			-- CPA3 CPA2 CPM CPH2 CPY CPC
        |			select
        |				plant,
        |				plant_date,
        |				capture_time,
        |				'Rolling' checkpoint,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				BHG,
        |				HG,
        |				NVL(BHG,0) + NVL(HG,0) AS ZS,
        |				rn
        |			from
        |			(
        |				-- CPA3 工厂 合格&不合格
        |				SELECT
        |					plant,
        |					cal_date plant_date,
        |					mdatumzeit capture_time,
        |					modell series_code_6,
        |					series_name series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					case
        |						when status0 = 'Q710' then 1
        |					end HG,
        |					case
        |						when status0 = 'Q705' then 1
        |					end BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, cal_date, status0 order by mdatumzeit) rn
        |				FROM mcp.mcp_fab_veh_fh01t04_hf
        |				WHERE plant = 'CPA3'
        |					AND status0 IN ('Q710','Q705') --Q705计数为不合格 RQ710计数为合格
        |				union all
        |				-- CPA2,CPM,CPH2,CPY,CPC
        |				-- 合格
        |				select
        |					b.plant,
        |					b.plant_date,
        |					b.Capture_time,
        |					b.series_code_6,
        |					b.series_name_6,
        |					b.werk,
        |					b.spj,
        |					b.kanr,
        |					1 HG,
        |					null BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by b.werk, b.spj, b.kanr, b.plant_date order by b.Capture_time) rn
        |				from
        |				(
        |					--依据werk,spj,kanr,获取result_value_id全为10的数据
        |					select werk, spj, kanr from mcp.mcp_fab_veh_result_info_hi
        |					where Result_value_id in('10','13')
        |					and substr(Result_name,-3) in ('702', '298',' 299')
        |					group by werk, spj, kanr having sum(CAST(Result_value_id AS INT))/count(1)=10
        |				)a
        |				join
        |				(
        |					--依据Result_value_id=10,按照Capture_time 升序进行排序，取top1 最早的时间
        |					select
        |						*,
        |						ROW_NUMBER() over(PARTITION by werk, spj, kanr order by Capture_time asc) rn
        |					from
        |						mcp.mcp_fab_veh_result_info_hi
        |					where Result_value_id = '10'
        |					and plant in ('CPM','CPH2','CPA2','CPC','CPY')
        |					and substr(Result_name,-3) in ('702', '298',' 299')
        |				)b on a.werk = b.werk
        |					and a.spj = b.spj
        |					and a.kanr = b.kanr
        |					and b.rn = 1
        |				-- 不合格车辆明细
        |				union all
        |				select
        |					plant,
        |					plant_date,
        |					Capture_time,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					null HG,
        |					1 BHG,
        |					null ZS,
        |					ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time) rn
        |				from
        |				(
        |					select
        |						*,
        |						ROW_NUMBER() over(PARTITION by werk, spj, kanr order by Capture_time asc) rn
        |					from mcp.mcp_fab_veh_result_info_hi
        |					where Result_value_id = '13'
        |					and plant in ('CPM','CPH2','CPA2','CPC','CPY')
        |					and substr(Result_name,-3) in ('702', '298',' 299')
        |				) ta WHERE RN = 1
        |			) t
        |			union all
        |			-- ZP7
        |			-- CPM CPH2 CPY CPC
        |			select
        |				a.plant,
        |				a.cal_date plant_date,
        |				a.mdatumzeit capture_time,
        |				'ZP7' checkpoint,
        |				a.modell series_code_6,
        |				a.series_name series_name_6,
        |				a.werk,
        |				a.spj,
        |				a.kanr,
        |				case when b.werk is not null then 1 end BHG,
        |				case when b.werk is null then 1 end HG,
        |				1 ZS,
        |				ROW_NUMBER() over(PARTITION by a.werk, a.spj, a.kanr, a.cal_date order by mdatumzeit) rn
        |			from
        |			(
        |				select * from mcp.mcp_fab_veh_fh01t04_hf
        |				where status0 = 'Z700'
        |				and plant IN ('CPM','CPH2','CPC','CPY')
        |				and cal_date >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 1 * 24 * 60 * 60, 'yyyy-MM-dd')
        |				and rn = 1
        |			) a
        |			left join
        |			(
        |				-- 获取所有不合格车辆明细
        |				SELECT DISTINCT werk,spj,kanr FROM
        |				(
        |				--步骤2 关联 result表（FH01TQC0）获取不合格记录
        |					select werk,spj,kanr from
        |					(
        |						select
        |							werk,
        |							spj,
        |							kanr,
        |							Capture_time,
        |							result_name,
        |							Result_value_id,
        |							ROW_NUMBER() over(PARTITION by werk, spj, kanr, result_name order by Capture_time desc) rn
        |						from
        |							mcp.mcp_fab_veh_result_info_hi
        |						where Capture_time > from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 45 * 24 * 60 * 60,'yyyy-MM-dd')
        |					) a where result_value_id = '13' and rn = 1
        |					union all
        |					-- 步骤3  关联 FH04TA06表获取不合格记录
        |					select werk,spj,kanr from
        |					(
        |						select
        |							werk,
        |							spj,
        |							kanr,
        |							check_value_id,
        |							ROW_NUMBER() over(PARTITION by werk, spj, kanr, check_name order by Capture_time desc) rn
        |						from mcp.mcp_fab_veh_fh04ta06_check_hi
        |						where Capture_time > from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 45 * 24 * 60 * 60,'yyyy-MM-dd')
        |					) a where check_value_id='4' and rn=1
        |					union all
        |					-- 步骤4 关联条码表FH01TQA5获取不合格记录
        |					select werk,spj,kanr from
        |					(
        |						select distinct
        |							werk,
        |							spj,
        |							kanr
        |						from mcp.mcp_fab_veh_fh01tqa5_hi
        |					) a
        |					union all
        |					-- 步骤5 关联defect表（数据湖表dw.fct_fab_veh_fh04ta01_defect_di）获取不合格记录
        |					select t1.werk,t1.spj,t1.kanr from
        |					(
        |						select
        |							werk,
        |							spj,
        |							kanr,
        |							geraetename_io,
        |							geraetename_nio
        |						from mcp.mcp_fab_veh_fh04ta01_defect_hi
        |						where defect_ts > from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 45 * 24 * 60 * 60,'yyyy-MM-dd')
        |					) t1
        |					join ods.mcp_pf_zp7_equipment_df t2 -- 根据设备号关联, 关联上的车子为不合格
        |					on t1.geraetename_io = t2.equipment or t1.geraetename_nio = t2.equipment
        |				) t
        |			)b
        |			ON A.werk = B.werk AND A.spj = B.spj AND A.kanr = B.kanr
        |		) t
        |		where rn = 1
        |		union all
        |		-- Torque
        |		-- CPA2 CPA3 CPM CPH2 CPY CPC
        |		select
        |			a.plant,
        |			a.capture_time,
        |			a.series_code_6,
        |			a.series_name_6,
        |			a.werk,
        |			a.spj,
        |			a.kanr,
        |			null BHG,
        |			null HG,
        |			case when a.rn = 1 and b.werk is not null then 1 end ZS,
        |			a.torque_BHG,
        |			a.torque_HG,
        |			a.torque_ZS,
        |			a.plant_date,
        |			'Torque' checkpoint
        |		from
        |		(
        |			SELECT
        |				plant,
        |				plant_date,
        |				capture_time,
        |				series_code_6,
        |				series_name_6,
        |				werk,
        |				spj,
        |				kanr,
        |				torque_BHG,
        |				torque_HG,
        |				torque_ZS,
        |				-- 记录车辆过点螺栓时间，取一辆车当天最早的时间
        |				ROW_NUMBER() over(PARTITION by werk, spj, kanr, plant_date order by Capture_time desc) rn
        |			FROM
        |			(
        |				select
        |					plant,
        |					plant_date,
        |					capture_time,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr,
        |					sum(BHG) torque_BHG,
        |					sum(HG) torque_HG,
        |					sum(NVL(BHG,0) + NVL(HG,0)) AS torque_ZS
        |				from
        |				(
        |					-- 合格螺栓明细
        |					select
        |						b.plant,
        |						b.plant_date,
        |						b.Capture_time,
        |						b.series_code_6,
        |						b.series_name_6,
        |						b.werk,
        |						b.spj,
        |						b.kanr,
        |						1 HG,
        |						null BHG,
        |						null ZS
        |					from
        |					(
        |						--依据result_name, werk, spj, kanr,获取result_value_id只为10的所有明细值
        |						select result_name, werk, spj, kanr
        |						from mcp.mcp_fab_veh_result_info_hi
        |						where Result_value_id in('10','13')
        |						and geraetename not regexp '%FL%'
        |						and case
        |								when plant in ('CPA2','CPA3','CPY','CPC') and result_name regexp '-|_VW' then plant
        |								when plant='CPM' and (result_name regexp '-' or (LENGTH(result_name)=10 and SUBSTR(result_name , -1 , 1)='D')) then plant
        |								when plant='CPH2' and result_name regexp '-'  then plant
        |							end is not null
        |						group by result_name, werk, spj, kanr having sum(CAST(Result_value_id AS INT))/count(1)=10
        |					)a
        |					join
        |					(
        |						--依据Result_value_id=10,按照Capture_time 升序进行排序，取top1 最早的时间
        |						select
        |							*,
        |							ROW_NUMBER() over(partition by werk,spj,kanr,result_name ORDER BY capture_time asc) rn
        |						from
        |							mcp.mcp_fab_veh_result_info_hi
        |						-- 记录只为10的数据，获取最早时间
        |						where Result_value_id = '10'
        |						and geraetename not regexp '%FL%'
        |						and case
        |								when plant in ('CPA2','CPA3','CPY','CPC') and result_name regexp '-|_VW' then plant
        |								when plant='CPM' and (result_name regexp '-' or (LENGTH(result_name)=10 and SUBSTR(result_name , -1 , 1)='D')) then plant
        |								when plant='CPH2' and result_name regexp '-'  then plant
        |							end is not null
        |					)b
        |					on a.result_name = b.result_name
        |					and a.werk = b.werk
        |					and a.spj = b.spj
        |					and a.kanr = b.kanr
        |					and b.rn = 1
        |					-- 不合格螺栓明细
        |					union all
        |					select
        |						plant,
        |						plant_date,
        |						Capture_time,
        |						series_code_6,
        |						series_name_6,
        |						werk,
        |						spj,
        |						kanr,
        |						null HG,
        |						1 BHG,
        |						null ZS
        |					from
        |					(
        |						select
        |							*,
        |							ROW_NUMBER() over(partition by werk,spj,kanr,result_name ORDER BY capture_time asc) rn
        |						from mcp.mcp_fab_veh_result_info_hi
        |						where Result_value_id = '13'
        |						and geraetename not regexp '%FL%'
        |						and case
        |								when plant in ('CPA2','CPA3','CPY','CPC') and result_name regexp '-|_VW' then plant
        |								when plant='CPM' and (result_name regexp '-' or (LENGTH(result_name)=10 and SUBSTR(result_name , -1 , 1)='D')) then plant
        |								when plant='CPH2' and result_name regexp '-'  then plant
        |							end is not null
        |					) b WHERE RN = 1
        |				) p
        |				group by
        |					plant,
        |					plant_date,
        |					capture_time,
        |					series_code_6,
        |					series_name_6,
        |					werk,
        |					spj,
        |					kanr
        |			)TA
        |		) a
        |		left join
        |		(
        |			select * from mcp.mcp_fab_veh_fh01t04_hf
        |			where status0 = 'M100' and rn = 1
        |		)b
        |		ON A.werk = B.werk AND A.spj = B.spj AND A.kanr = B.kanr AND A.plant_date = B.cal_date
        |	) p1
        |	left join
        |	(
        |		-- 工厂作息时间
        |		select
        |			factory,
        |			start_date,
        |			end_date,
        |			start_time,
        |			end_time,
        |			nvl(cast(substr(start_time, 1,2) as int),0) * 60 + nvl(cast(substr(start_time, 4,2) as int),0) as mi
        |		from analytical_db_manual_table.mcp_pf_factory_work_schedule_df
        |	)p2
        |	on p1.plant = p2.factory
        |	and substr(p1.capture_time,1,10) >= p2.start_date
        |	and substr(p1.capture_time,1,10) <= p2.end_date
        |) t where plant_date >= from_unixtime(unix_timestamp('${bizdate}','yyyyMMdd') - 1 * 24 * 60 * 60, 'yyyy-MM-dd')
        |and plant_date <= CURRENT_DATE()
        |"""
        .stripMargin)
    println("---------计算完成--------")

    sparkHive.stop()
  }
}
