#!/bin/bash
# Function:
#   sync up domestic sto ods data to dwd layer
# History:
# 2021-05-17    Donny   v1.0    init
# 2021-05-24    Donny   v1.1    add data rule
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
# 如果是输入的日期按照取输入日期，默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
echo "start syncing domestic sto data into DWD layer on ${sync_date} .................."
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk '{print $8}' | grep -oP 20'[^ ]*' |awk 'BEGIN {max = 0} {if ($1 > max) max=$1} END {print max}'`
# 1 Hive SQL string
sto_sql="
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;
set hive.exec.mode.local.auto=true; --本地执行
drop table if exists tmp_dwd_fact_domestic_sto_info_ods_sto;
create table tmp_dwd_fact_domestic_sto_info_ods_sto stored as orc as
    select 
        sto_no
       ,sto_ceate_dt
       ,sto_create_by
       ,sto_update_dt
       ,sto_updated_by
       ,sto_status
       ,remarks
       ,sto_type
       ,sto_reason
       ,ship_from_plant
       ,ship_to_plant
       ,stoline_no
       ,material
       ,qty
       ,unit
    from ${target_db_name}.ods_domestic_sto --源表 TRANS_DomesticTransaction
    where dt='$sync_date'
	and month(sto_ceate_dt)>1;	
drop table if exists tmp_dwd_fact_domestic_sto_info_sku;
create table tmp_dwd_fact_domestic_sto_info_sku stored as orc as	
---没有重复不需要去重已验证
	    select 
		 material_code
        ,division_display_name
        ,default_location
    from ${target_db_name}.dwd_dim_material  --源表主表MDM_MaterialMaster 全量更新,次表MDM_DivisionMaster 全量更新
    where dt ='$dwd_dim_material_maxdt'
    ---( select max(dt) from ${target_db_name}.dwd_dim_material where dt>=date_sub('$sync_date',10) ) ;
insert overwrite table ${target_db_name}.dwd_fact_domestic_sto_info partition(dt)
select  
     ods_sto.sto_no
    ,ods_sto.sto_ceate_dt  -- create_datetime
    ,ods_sto.sto_create_by -- created_by
    ,ods_sto.sto_update_dt -- update_datetime
    ,ods_sto.sto_updated_by -- updated_by
    ,ods_sto.ship_from_plant
    ,ods_sto.ship_to_plant
    ,ods_sto.sto_status --order_status
    ,ods_sto.remarks
    ,ods_sto.sto_type -- order_type
    ,ods_sto.sto_reason -- order_reason
    ,ods_sto.stoline_no -- line_number
    ,ods_sto.material
    ,ods_sto.qty
    ,nvl(ods_sto.unit, 'ea')
    ,sku.division_display_name --financial_dimension_id
    ,0
    ,sku.default_location
    ,SUBSTR(ods_sto.sto_ceate_dt,1,10)
from tmp_dwd_fact_domestic_sto_info_ods_sto ods_sto
left outer join tmp_dwd_fact_domestic_sto_info_sku sku on ods_sto.material=sku.material_code; "
#删除临时表数据
delete_sql="
drop table tmp_dwd_fact_domestic_sto_info_ods_sto;
drop table tmp_dwd_fact_domestic_sto_info_sku;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
$hive -e "$delete_sql"
echo "End syncing domestic sto data into DWD layer on ${sync_date} .................."