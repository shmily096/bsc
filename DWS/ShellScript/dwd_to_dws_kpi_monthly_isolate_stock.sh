#!/bin/bash
# Function:
#   sync up dws_monthly_isolate_stock 
# History:
# 2022-05-30    slc   v2.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)
year_month=$(date  +'%Y-%m')
this_month=`date -d "${sync_date[day]}" +%Y-%m-01`

echo "start syncing dws_kpi_monthly_isolate_stock data into DWS layer on ${sync_date[month]} : ${sync_date[year]}"

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
 
drop table if exists tmp_dws_kpi_monthly_isolate_stockonhand_stock;
create table tmp_dws_kpi_monthly_isolate_stockonhand_stock stored as orc as
  --- onhand on last day from last month
  select plant
      , storage_loc as location 
      , material 
      , batch
      , unrestricted
      , inspection 
	  , blocked_material
	  , dt
   from dwd_fact_inventory_onhand 
  where (pmod(DATEDIFF(dt,'1920-01-01')-3,7)='1' or  dt=last_day(dt))---取每周一和每月最后一天
  -- (pmod(DATEDIFF(dt,'1920-01-01')-3,7)='1' or  SUBSTR(dt,9)='01')---取每周一和每月1号
    and dt >=  date_add('${sync_date[day]}',-93)--每月一号
    and inventory_type = 'DC'
    and plant in ('D835','D836','D837','D838');

drop table if exists tmp_dws_kpi_monthly_isolate_stockstock_combined;
create table tmp_dws_kpi_monthly_isolate_stockstock_combined stored as orc as
 --- transposed
 select plant
      , location 
      , 'UR' as status 
      , material 
      , batch
      , unrestricted as qty
      , concat(plant,'|',location,'|UR') as name 
	  , dt
   from tmp_dws_kpi_monthly_isolate_stockonhand_stock 
  where unrestricted > 0
 union all 
  select plant
      , location 
      , 'QI' as status 
      , material 
      , batch
      , inspection as qty
      , concat(plant,'|',location,'|QI') as name
	  , dt
   from tmp_dws_kpi_monthly_isolate_stockonhand_stock 
  where inspection > 0 
  union all 
   select plant
      , location 
      , 'BLK' as status 
      , material 
      , batch
      , blocked_material as qty
      , concat(plant,'|',location,'|BLK') as name 
	  , dt
   from tmp_dws_kpi_monthly_isolate_stockonhand_stock 
  where blocked_material > 0;


insert overwrite table opsdw.dws_kpi_monthly_isolate_stock PARTITION(dt)
select  s.name
	  ,s.plant
      ,s.location 
      ,s.status 
      ,s.material 
      ,s.batch
      ,s.qty      
	  ,d.flag 
	  ,nvl(d.supplier,'')as  supplier
    ,CASE WHEN d.flag = '\u6b63\u5e38' THEN 'WH005' ---正常
          WHEN  d.flag = '\u9694\u79bb' THEN 'WH006' ---隔离
          end as kpi_code
	  ,s.dt
  from tmp_dws_kpi_monthly_isolate_stockstock_combined s 
left join (select distinct name,flag,case when plant='D837' THEN 'TJ DC' ELSE supplier END AS supplier
     from opsdw.dwd_sap_Warehouse_attribute 
			where dt=(select max(dt) from opsdw.dwd_sap_Warehouse_attribute)) d on s.name = d.name
  where month(s.dt)>=1

"
###删除所有临时表
delete_tmp="
drop table tmp_dws_kpi_monthly_isolate_stockstock_combined;
drop table tmp_dws_kpi_monthly_isolate_stockonhand_stock;
"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_kpi_monthly_isolate_stock data into DWS layer on ${sync_date} .................."		
