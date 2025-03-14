#!/bin/bash
export LANG="en_US.UTF-8"
# export LC_ALL=zh_CN.GB2312;
# export LANG=zh_CN.GBK
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)
year_month=$(date  +'%Y-%m')
if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
fi

echo "start syncing dwd_fact_work_order_qr_code_mapping_new data into DWD layer on ${sync_date} .................."
####创建临时表
create_tmp="
drop table tmp_mov_qty;
CREATE EXTERNAL TABLE opsdw.tmp_mov_qty(
explanation string, 
  plant string, 
  delivery_plant string, 
  stock_location string, 
  default_location string, 
  material string, 
  batch string, 
  qty bigint, 
  movement_type string, 
  delivery_no string, 
  enter_date string, 
  enter_datetime string, 
  enter_dateunixtime bigint, 
  supplier string , 
  delivery_supplier string , 
  dt string)
COMMENT 'kpi_putawy_的前置临时表'
PARTITIONED BY ( 
  delivery_moven_type string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://hadoop-master:8020/bsc/opsdw/dws/tmp_mov_qty'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'last_modified_by'='root', 
  'last_modified_time'='1651557825', 
  'parquet.compression'='lzo', 
  'transient_lastDdlTime'='1659511167');
"
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
# 1 Hive SQL string
lines_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;



drop table if exists tmp_Warehouse;
create table  tmp_Warehouse stored as orc as 
    select  plant 
        ,  location
        ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END AS supplier
    from dwd_sap_warehouse_attribute  ---库位表
    where dt = (select max(dt) from dwd_sap_warehouse_attribute)
      and sap_status = 'Active'  --有效
    group by  plant 
        ,  location
        ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END;
		 
drop table if exists tmp_cp ;
create table  tmp_cp stored as orc as 
	select 
		material_code ,
		default_location,
		delivery_plant
	from opsdw.dwd_dim_material --产品表
	where dt='$dwd_dim_material_maxdt'
	---(select max(dt) from opsdw.dwd_dim_material where dt>=date_sub('$sync_date',10))
	and delivery_plant is not null 
	and default_location is not null;

---转仓表关联supplir，，，取的是目的地仓库和库位对应的supplir
drop table if exists tmp_zc ;
create table tmp_zc stored as orc as 
	select
		dfd.actual_plant,
		dfd.delivery_no,
		dfd.reference_dn_number,
		dfd.ship_to_location,
		Warehouse.supplier
	from (
		select 
		case when ship_to_plant = 'PD838' then 'D838'
				when ship_to_plant = 'PD836' then 'D836'
			end as actual_plant
		, delivery_no 
		, reference_dn_number
		, ship_to_location
		from opsdw.dwd_fact_domestic_sto_dn_info 
		where dt  >= date_add('${end_date}',-365)
		and dt  <= '${end_date}'  ---- 2 weeks
		and  ship_to_plant IN ('PD836','PD838')
		group by 
		case when ship_to_plant = 'PD838' then 'D838'
				when ship_to_plant = 'PD836' then 'D836'
			end 
		, delivery_no 
		,reference_dn_number
		, ship_to_location)dfd
	left join tmp_Warehouse Warehouse--库位表
		on dfd.actual_plant=Warehouse.plant and dfd.ship_to_location=Warehouse.location;

drop table if exists tmp_cp_and_Warehouse ;
	----产品表关联库位表取supplir
create table  tmp_cp_and_Warehouse stored as orc as 
select 
	cp.material_code,
	cp.default_location,
	cp.delivery_plant,
	Warehouse.supplier
from tmp_cp cp --产品表取delivery_plant
left join tmp_Warehouse Warehouse --库位表
	on cp.delivery_plant=Warehouse.plant and cp.default_location=Warehouse.location;

drop table if exists tmp_inv_transaction ;
create table  tmp_inv_transaction stored as orc as 
select m.plant
    ,coalesce(zc.actual_plant,zcc.actual_plant,m.delivery_plant)as delivery_plant
     , m.stock_location
     , coalesce(zc.ship_to_location,zcc.ship_to_location,m.default_location) as default_location
     , m.movement_type
	 , m.new_movement_type
     , m.material
     , m.batch
     , m.qty
     , m.enter_date
     , m.enter_datetime
     , m.enter_dateunixtime
     , m.supplier
    , coalesce(zc.supplier,zcc.supplier,m.defult_supplier)as delivery_supplier
     , m.dt
     , coalesce(zcc.delivery_no, m.delivery_no) as delivery_no --运单号 
     , d.kpiqtyflag
 from (
      select a.plant --仓库编号
          ,c.delivery_plant
          ,b.supplier
          ,c.supplier as defult_supplier
          , c.default_location 
          ,a.stock_location
          , case when a.movement_type ='311' then '321' else a.movement_type end as new_movement_type 
		  ,a.movement_type--库存类型
          , a.material  --产品编号
          , a.batch --批次
          , a.qty   --数量
          , case when a.delivery_no= '' then a.original_reference else delivery_no end as delivery_no   --运单号 
          , a.enter_date    --上架日期
          , concat(a.enter_date,' ',a.mov_time) as enter_datetime  --上架时间
          , unix_timestamp(concat(a.enter_date,' ',a.mov_time)) as enter_dateunixtime --上架时间的数字格式
          , a.dt
       from opsdw.dwd_fact_inventory_movement_trans a   --库存交易表
       left join tmp_Warehouse b on a.plant=b.plant and a.stock_location=b.location --仓库库位属性表  
       left join tmp_cp_and_Warehouse                   c on a.material=c.material_code --仓库产品库位属性表                
       where   
	   -- a.dt>='2022-01-01'
       a.dt  >= date_add('${end_date}',-93)
       and a.dt  <= '${end_date}'  ---- 2 weeks
        and case when a.movement_type in ('321')  then a.qty else 1 end >0 
    ) m 
 left join (SELECT 
                movementtype,
                kpiqtyflag 
            from dwd_dim_movetype --库存操作代码对照表
            where dt=(SELECT max(dt) from dwd_dim_movetype) and kpiqtyflag <> '999') d on m.movement_type = d.movementtype
left join tmp_zc zc on m.delivery_no=zc.delivery_no
left join tmp_zc zcc on cast(m.delivery_no as decimal(16,0))=cast(zcc.reference_dn_number as decimal(16,0));


insert overwrite table ${target_db_name}.tmp_mov_qty partition(delivery_moven_type)
   select d.explanation
        , it.plant
		, it.delivery_plant		
        , it.stock_location
		, it.default_location
        , it.material 
        , it.batch 
        , it.qty
		, it.new_movement_type as movement_type
		, case when spi.material is not null then concat(it.material,it.batch) 
				else coalesce(dn.outbond_dn,it.delivery_no) end as delivery_no
        , it.enter_date
        , it.enter_datetime
        , it.enter_dateunixtime
        , it.supplier
		, it.delivery_supplier
        , it.dt
		,CONCAT(it.delivery_plant,it.movement_type) as delivery_moven_type
     from (
        select *
          from dwd_dim_kpiqtyflag  --KPI类型表
         where qtyflag <> '999') d
   inner join  tmp_inv_transaction it on it.kpiqtyflag = d.qtyflag
   left join (SELECT distinct  material
				from tmp_inv_transaction
				where movement_type='981')spi on it.material=spi.material
	left join (SELECT distinct cast(int(inbound_dn) as string)as inbound_dn 
					,outbond_dn
			from opsdw.ods_inbound_outbound_dn_mapping
			where dt>=date_add('$end_date',-365) and outbond_dn is not null 
				and substr(int(inbound_dn),0,2)='18')dn on cast(it.delivery_no as decimal(16,0))=cast(dn.inbound_dn as decimal(16,0))
		 ;"
putawy_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

drop table if exists tmp_mov_ind;
create table tmp_mov_ind stored as orc as
select
		distinct
		plant
		,stock_location ---改为321
		,delivery_plant 
		,default_location
		,supplier
		,material 
		,batch 
		,delivery_no
		,qty
		,put_gap_qty-in_qty as qty_gap
		,start_time
		,end_time
		,CAST((end_timeunixtime - start_timeunixtime)/3600 as float) as processtime_cd  --自然小时
		,myudf(start_time,end_time)*24  as no_work_hr    --其中非工作日多少小时 -24就是有空值
		,case when CAST((end_timeunixtime - start_timeunixtime)/3600 as float)
						- myudf(start_time,end_time)*24 <0 then 0
					else CAST((end_timeunixtime - start_timeunixtime)/3600 as float)
						- myudf(start_time,end_time)*24
				end as process_wd_m --剔除非工作日多少小时 
		, 'migo2putaway' as process_category
		, 'WH015' as KPICode
		, dt
	from(
		select 
		 x.plant
		,x.stock_location ---改为321
		,x.delivery_plant 
		,x.default_location
		,x.supplier
		,x.material 
		,x.batch 
		,x.delivery_no
		,x.movement_type  
		,sum(case when x.movement_type = '321'  then qty else 0 end)over(partition by x.plant, stock_location,material,batch,delivery_no) as qty
		,x.enter_datetime
		,x.enter_dateunixtime
		,sum(case when x.movement_type = '321'  then qty else 0 end)over(partition by x.plant ,stock_location,material,batch,delivery_no) as put_qty
		,sum(case when x.movement_type = '321'  then qty else 0 end)over(partition by x.plant ,material,batch,delivery_no) as put_gap_qty
		,sum(case when x.movement_type IN ('101','981') then qty else 0 end)over(partition by x.plant ,material,batch,delivery_no) as in_qty
		,max(case when x.movement_type IN ('101','981') then enter_datetime end)over(partition by x.plant ,material,batch,delivery_no) as start_time
		,max(case when x.movement_type IN ('101','981') then enter_dateunixtime end)over(partition by x.plant ,material,batch,delivery_no) as start_timeunixtime
		,max(case when x.movement_type = '321' then enter_datetime end)over(partition by x.plant ,stock_location,material,batch,delivery_no) as end_time
		,max(case when x.movement_type = '321' then enter_dateunixtime end)over(partition by x.plant ,stock_location,material,batch,delivery_no) as end_timeunixtime
		,x.dt
		from tmp_mov_qty x
		inner join opsdw.dwd_putawy_plant_defult_plant y on x.plant=y.plant and x.delivery_plant=y.defult_plant
		where x.movement_type in ('321','101','981')  ---后面这里变成一张维表
		and x.explanation<>'Localization'
		)x
		where x.movement_type='321'
	union all 
	 select plant 
		  , stock_location
		  , delivery_plant
		  , default_location
		  , nvl(supplier,'Unknown')
          , material 
          , batch 
		  , delivery_no		  
		  , sum(qty) as qty 
		  , 0 as qty_gap
		   , case when plant = 'D835' and stock_location = 'LP05' then 'STO' else 'Non-STO' end as start_time  ---'WH002.1'里面的用作一个flag
		  , null as end_time
		  , 0 as processtime_cd
		  , 0 as no_work_hr
		  , 0 as process_wd_m
		  , explanation as process_category
		  , case when explanation = 'goods_receive'     then 'WH001'
		         when explanation = 'putaway'           then 'WH002.1'
                 when explanation = 'sales_goods_issue' then 'WH003'
				 when explanation = 'stock_transfer'    then 'WH004' end as KPICode
		  , dt
	   from tmp_mov_qty
	 group by plant 
		  , stock_location
		  , delivery_plant
		  , default_location
		  , nvl(supplier,'Unknown')
          , material 
          , batch 
		  , delivery_no
		  , explanation
		  , case when explanation = 'goods_receive'     then 'WH001'
		         when explanation = 'putaway'           then 'WH002.1'
                 when explanation = 'sales_goods_issue' then 'WH003'
				 when explanation = 'stock_transfer'    then 'WH004' end
		  ,dt
	union all 
		select 
			substr(plant_id,1,4) as plant 
			,'' as stock_location
			,'' as delivery_plant
			,'' as default_location
			,	 case when substr(plant_id,1,4)='D835' THEN 'SLC' 
                      WHEN substr(plant_id,1,4)='D838' THEN 'YH'
                      WHEN substr(plant_id,1,4)='D836' THEN 'CD-NonBounded'
                      WHEN substr(plant_id,1,4)='D837' THEN 'TJ DC'
                      ELSE 'NO' END as supplier
			, material
			, batch
			, dn_no as delivery_no			
			--, count(distinct qr_code) as qty
			,sum(releasedqty) as qty
			, 0 as qty_gap
			, null as start_time
			, null as end_time
			, 0 as processtime_cd
			, 0 as no_work_hr
			, 0 as process_wd_m
			, 'localization' as process_category	 
			, 'WH002' as KPIcode
			, dt 
	  from dwd_trans_workorder_ordz
	 where 
	 --dt >='2022-04-01'
	 dt >= date_add('${end_date}',-93)
	  and dt <= '${end_date}'
	group by plant_id
		   , dn_no
		   , material
		   , batch
		   , dt;
insert overwrite table opsdw.dws_kpi_stock_putaway_time partition(dt)
select
	plant, 
	stock_location,
	delivery_plant,
	default_location,
	supplier,
	material,
	batch,
	delivery_no, 
	qty, 
	qty_gap,
	start_time,
	end_time,
	processtime_cd,
	case when no_work_hr=-24 then 0 else no_work_hr end as no_work_hr, 
	process_wd_m, 
	process_category, 
	kpicode, 
	dt
from tmp_mov_ind
where dt >= date_add('${end_date}',-30) and dt>='2022-04-01'
and substr(kpicode,1,1)='W'
and case when kpicode='WH015' AND end_time IS NULL THEN 0 ELSE 1 END >0;"

###删除所有临时表
delete_tmp="
drop table tmp_Warehouse;
drop table tmp_cp;
drop table tmp_zc;
drop table tmp_cp_and_Warehouse;
drop table tmp_inv_transaction;

drop table tmp_mov_ind;
"

# 2. 执行加载数据SQL
##先创建一个临时表
echo " first $create_tmp"
$hive -e "$create_tmp"
#第一部分
echo " two $lines_sql"
$hive -e "$lines_sql"
#第二部分
echo " three $putawy_sql"
$hive -e "$putawy_sql"
#第三部分收尾删除所有临时表
echo "four $delete_tmp"
#$hive -e "$delete_tmp"

echo "End syncing dwd_fact_work_order_qr_code_mapping_new data into DWD layer on ${sync_date} .................."