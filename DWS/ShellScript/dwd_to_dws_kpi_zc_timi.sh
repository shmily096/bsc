#!/bin/bash
# Function:
#   sync up dws_kpi_zc_timi 
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
fi

echo "start syncing dws_kpi_zc_timi data into DWS layer on ${sync_date} : ${sync_date[year]}"

# billed:
# dned:

sto_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

drop table if exists tmp_dws_kpi_zc_timi_Warehouse;
create table  tmp_dws_kpi_zc_timi_Warehouse stored as orc as 
    select plant
        ,  location
        ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END AS supplier
    from dwd_sap_warehouse_attribute 
    where dt = (select max(dt) from dwd_sap_warehouse_attribute)
      and sap_status = 'Active'  --有效
    group by  plant
         ,  location
         ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END;
drop table if exists tmp_dws_kpi_zc_timi_zc;
create table  tmp_dws_kpi_zc_timi_zc stored as orc as 
select
  a.sto_no
, a.delivery_no	--outbound运单号
, a.reference_dn_number --inbound运单号
, a.chinese_dncreatedt as create_datetime --运单创建时间
, a.actual_migo_date	--运单收货时间
, a.pgi_datetime	--运单实际发货时间
, a.delivery_mode
, a.dn_status	--运单状态
, a.ship_from_location	--库位
, a.ship_from_plant	--从哪个仓库发的
, c.supplier as from_supplier
, d.supplier as to_supplier
, a.ship_to_plant	--发到哪个仓库
, a.ship_to_location --库位
, b.material --产品编号
, b.batch  
, b.qty
,date_format(a.pgi_datetime,'yyyy-MM-dd')  as dt   
from (select* from opsdw.dwd_fact_domestic_sto_dn_info where dt>= date_add('$sync_date',-93) and actual_migo_date is not null)a 
left join (select 
                sto_no
                ,delivery_no
                ,material
                ,batch
                ,sum(case when trim(qr_code) <> ''and qr_code is not null then 1 else qty end) as qty
            from opsdw.dwd_fact_domestic_sto_dn_detail
            where dt>= date_add('$sync_date',-93)
                group by 
                sto_no
                ,delivery_no
                ,material
                ,batch) b on a.sto_no=b.sto_no and a.delivery_no=b.delivery_no   
left join tmp_dws_kpi_zc_timi_Warehouse c on a.ship_from_plant=c.plant and a.ship_from_location=c.location --仓库库位属性表     
left join tmp_dws_kpi_zc_timi_Warehouse d on a.ship_to_plant=d.plant and a.ship_to_location=d.location --仓库库位属性表     
;


insert overwrite table opsdw.dws_kpi_zc_timi partition(dt_yearmon,dt_week,dt)
select
  sto_no
, delivery_no	--outbound运单号
, reference_dn_number --inbound运单号
, min(create_datetime) as create_datetime --运单创建时间
, min(actual_migo_date) as actual_migo_date	--运单收货时间
, min(pgi_datetime) as pgi_datetime	--运单实际发货时间
, delivery_mode
, dn_status	--运单状态
, ship_from_location	--库位
, ship_from_plant	--从哪个仓库发的
, from_supplier
, to_supplier
, ship_to_plant	--发到哪个仓库
, ship_to_location --库位
, material --产品编号
, batch  
, sum(case when pgi_datetime is not null then qty end) qty
, CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then create_datetime end)))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
, myudf(min(case when actual_migo_date is not null then create_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24  as no_work_hr    --其中非工作日多少小时
, case when CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then create_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when actual_migo_date is not null then create_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24 <0 then 0
       else CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then create_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when actual_migo_date is not null then create_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24  
       end as lt_dw_hr --剔除非工作日多少小时
, 'WH018' as KPI_no
, date_format(min(dt),'yyyy-MM')as dt_yearmon
, weekofyear(min(dt) ) as dt_week
, min(dt) as dt
from tmp_dws_kpi_zc_timi_zc zc
where zc.dt>='2022-04-01'
group by 
	  sto_no
	, delivery_no	--outbound运单号
	, reference_dn_number --inbound运单号
	, delivery_mode
	, dn_status	--运单状态
	, ship_from_location	--库位
	, ship_from_plant	--从哪个仓库发的
	, from_supplier
	, to_supplier
	, ship_to_plant	--发到哪个仓库
	, ship_to_location --库位
	, material --产品编号
	, batch 
union all 
select
  sto_no
, delivery_no	--outbound运单号
, reference_dn_number --inbound运单号
, min(create_datetime) as create_datetime --运单创建时间
, min(actual_migo_date) as actual_migo_date	--运单收货时间
, min(pgi_datetime) as pgi_datetime	--运单实际发货时间
, delivery_mode
, dn_status	--运单状态
, ship_from_location	--库位
, ship_from_plant	--从哪个仓库发的
, from_supplier
, to_supplier
, ship_to_plant	--发到哪个仓库
, ship_to_location --库位
, material --产品编号
, batch  
, sum(case when pgi_datetime is not null then qty end) qty
, CAST((unix_timestamp(min(case when pgi_datetime is not null then pgi_datetime end)) - unix_timestamp(min(case when pgi_datetime is not null then create_datetime end)))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
, myudf(min(case when pgi_datetime is not null then create_datetime end),min(case when pgi_datetime is not null then pgi_datetime end))*24  as no_work_hr    --其中非工作日多少小时
, case when CAST((unix_timestamp(min(case when pgi_datetime is not null then pgi_datetime end)) - unix_timestamp(min(case when pgi_datetime is not null then create_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when pgi_datetime is not null then create_datetime end),min(case when pgi_datetime is not null then pgi_datetime end))*24 <0 then 0
       else CAST((unix_timestamp(min(case when pgi_datetime is not null then pgi_datetime end)) - unix_timestamp(min(case when pgi_datetime is not null then create_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when pgi_datetime is not null then create_datetime end),min(case when pgi_datetime is not null then pgi_datetime end))*24  
       end as lt_dw_hr --剔除非工作日多少小时
, 'WH018.0' as KPI_no
, date_format(min(dt),'yyyy-MM')as dt_yearmon
, weekofyear(min(dt) ) as dt_week
, min(dt) as dt
from tmp_dws_kpi_zc_timi_zc zc
where zc.dt>='2022-04-01'
group by 
	  sto_no
	, delivery_no	--outbound运单号
	, reference_dn_number --inbound运单号
	, delivery_mode
	, dn_status	--运单状态
	, ship_from_location	--库位
	, ship_from_plant	--从哪个仓库发的
	, from_supplier
	, to_supplier
	, ship_to_plant	--发到哪个仓库
	, ship_to_location --库位
	, material --产品编号
	, batch 
union all 
select
  sto_no
, delivery_no	--outbound运单号
, reference_dn_number --inbound运单号
, min(create_datetime) as create_datetime --运单创建时间
, min(actual_migo_date) as actual_migo_date	--运单收货时间
, min(pgi_datetime) as pgi_datetime	--运单实际发货时间
, delivery_mode
, dn_status	--运单状态
, ship_from_location	--库位
, ship_from_plant	--从哪个仓库发的
, from_supplier
, to_supplier
, ship_to_plant	--发到哪个仓库
, ship_to_location --库位
, material --产品编号
, batch  
, sum(case when actual_migo_date is not null then qty end) qty
, CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then pgi_datetime end)))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
, myudf(min(case when actual_migo_date is not null then pgi_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24  as no_work_hr    --其中非工作日多少小时
, case when CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then pgi_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when actual_migo_date is not null then pgi_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24 <0 then 0
       else CAST((unix_timestamp(min(case when actual_migo_date is not null then actual_migo_date end)) - unix_timestamp(min(case when actual_migo_date is not null then pgi_datetime end)))/(60 * 60) AS float)
             - myudf(min(case when actual_migo_date is not null then pgi_datetime end),min(case when actual_migo_date is not null then actual_migo_date end))*24  
       end as lt_dw_hr --剔除非工作日多少小时
, 'WH018.1' as KPI_no
, date_format(min(dt),'yyyy-MM')as dt_yearmon
, weekofyear(min(dt) ) as dt_week
, min(dt) as dt
from tmp_dws_kpi_zc_timi_zc zc
where zc.dt>='2022-04-01'
group by 
	  sto_no
	, delivery_no	--outbound运单号
	, reference_dn_number --inbound运单号
	, delivery_mode
	, dn_status	--运单状态
	, ship_from_location	--库位
	, ship_from_plant	--从哪个仓库发的
	, from_supplier   ---supplir用的from supplir,tableau
	, to_supplier
	, ship_to_plant	--发到哪个仓库
	, ship_to_location --库位
	, material --产品编号
	, batch 
"
delete_tmp="
drop table tmp_dws_kpi_zc_timi_Warehouse;
drop table tmp_dws_kpi_zc_timi_zc;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_kpi_zc_timi data into DWS layer on ${sync_date} .................."