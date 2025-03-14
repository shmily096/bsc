#!/bin/bash
# Function:
#   sync up dws_kpi_sales_waybill_timi 
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

echo "start syncing dws_kpi_sales_waybill_timi data into DWS layer on ${sync_date} : ${sync_date[year]}"

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

drop table if exists tmp_dws_kpi_sales_waybill_timi_Warehouse;
create table  tmp_dws_kpi_sales_waybill_timi_Warehouse stored as orc as 
    select plant
        ,  location
        ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END AS supplier
    from dwd_sap_warehouse_attribute 
    where dt = (select max(dt) from dwd_sap_warehouse_attribute)
      and sap_status = 'Active'  --有效
    group by  plant
         ,  location
         ,  case when plant='D837' THEN 'TJ DC' ELSE supplier END ;

drop table if exists tmp_dws_kpi_sales_waybill_timi_a;
create table  tmp_dws_kpi_sales_waybill_timi_a stored as orc as 
select * from opsdw.dwd_fact_sales_order_dn_info 
                where dt>= date_add('$sync_date',-93) and dt>='2022-04-01'
                and actual_gi_date is not null ;
drop table if exists tmp_dws_kpi_sales_waybill_timi_b;
create table  tmp_dws_kpi_sales_waybill_timi_b stored as orc as 
select 
	dn.so_no
	,dn.delivery_id
	,dn.material
	,dn.batch 
  ,dn.chinese_dncreatedt                            
	,sum(case when trim(dn.qr_code) <> ''and dn.qr_code is not null then 1 else dn.qty end) as qty
from opsdw.dwd_fact_sales_order_dn_detail dn
where dn.dt>= date_add('$sync_date',-93) and dt>='2022-04-01'
group by 
	dn.so_no
	,dn.delivery_id
	,dn.material
	,dn.batch 
  ,dn.chinese_dncreatedt  ;

				
insert overwrite table opsdw.dws_kpi_sales_waybill_timi partition(dt_yearmon,dt_week,dt)
select
  a.so_no
, a.delivery_id
, a.chinese_dncreatedt as created_datetime 
, a.actual_gi_date  
, a.receiving_confirmation_date  --收货时间
, a.ship_to_address   --收货地址  
, a.pick_location_id
, a.plant
, nvl(c.supplier,case when a.plant='D835' THEN 'SLC' 
                      WHEN a.plant='D838' THEN 'YH'
                      WHEN a.plant='D836' THEN 'CD-NonBounded'
                      WHEN a.plant='D837' THEN 'TJ DC'
                      ELSE 'NO' END)
, b.material 
, b.batch  
, b.qty
, CAST((unix_timestamp(actual_gi_date) - unix_timestamp(a.chinese_dncreatedt))/(60 * 60) AS float)  as lt_cd_hr
, myudf(a.chinese_dncreatedt,a.actual_gi_date)*24  as no_work_hr    
, case when CAST((unix_timestamp(actual_gi_date) - unix_timestamp(a.chinese_dncreatedt))/(60 * 60) AS float)
             - myudf(a.chinese_dncreatedt,a.actual_gi_date)*24 <0 then 0
       else CAST((unix_timestamp(actual_gi_date) - unix_timestamp(a.chinese_dncreatedt))/(60 * 60) AS float)
             - myudf(a.chinese_dncreatedt,a.actual_gi_date)*24  
       end as lt_dw_hr  --如果结果是-1就是其中有空值或者null值，如果结果小于=0就是当天或者休息日发完的
, 'WH017' as KPI_no
, date_format(a.dt,'yyyy-MM')as dt_yearmon
, weekofyear(a.dt ) as dt_week
, a.dt 
from tmp_dws_kpi_sales_waybill_timi_a a 
left join tmp_dws_kpi_sales_waybill_timi_b b on a.so_no=b.so_no and a.delivery_id=b.delivery_id and a.chinese_dncreatedt=b.chinese_dncreatedt
 left join tmp_dws_kpi_sales_waybill_timi_Warehouse c on a.plant=c.plant and a.pick_location_id=c.location --仓库库位属性表     

"
###删除所有临时表
delete_tmp="
drop table tmp_dws_kpi_sales_waybill_timi_a;
drop table tmp_dws_kpi_sales_waybill_timi_b;
drop table tmp_dws_kpi_sales_waybill_timi_Warehouse;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_kpi_sales_waybill_timi data into DWS layer on ${sync_date} .................."	
