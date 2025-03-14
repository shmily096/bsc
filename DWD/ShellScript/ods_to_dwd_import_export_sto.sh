#!/bin/bash
# Function:
#   sync up import and export sto ods data to dwd layer
# History:
# 2021-05-12    Donny   v1.0    init
# 2021-05-13    Donny   v1.1    completed the shell script

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

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi

echo "start syncing import and export sto data into DWD layer on $sync_year $sync_date.................."

# 1 Hive SQL string
# 数据清洗备注：
# a. Unit 如果为空，默认为ea

sto_sql="
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
--set hive.exec.parallel.thread.number=8;  

insert overwrite table ${target_db_name}.dwd_fact_import_export_sto partition(dt)
select  distinct
        ix_sto.sto_no
       ,ix_sto.sto_create_dt
       ,ix_sto.sto_update_dt
       ,ix_sto.sto_created_by
       ,ix_sto.sto_updated_by
       ,ix_sto.sto_status
       ,ix_sto.sto_type
       ,ix_sto.sto_order_reason
       ,ix_sto.order_remarks
       ,ix_sto.ship_from_plant
       ,ix_sto.ship_to_plant
       ,ix_sto.sto_line_no
       ,ix_sto.material
       ,ix_sto.qty
       ,nvl(ix_sto.unit, 'ea')  
       ,0.0
       ,0.0
       ,date_format(ix_sto.sto_create_dt,'yyyy-MM-dd')
from
(
    select *
    from ${target_db_name}.ods_import_export_transaction --源表TRANS_ImportExportTransaction增量更新
    where dt='$sync_date'
    and year(sto_create_dt)='$sync_year'
) ix_sto
; 
"
# 2. 执行SQL，并判断查询结果是否为空
count=`$hive -e "select count(*) from ods_import_export_transaction where dt='$sync_date'and substr(sto_create_dt,1,1)='2'" | tail -n1`

if [ $count -eq 0 ]; then
  echo "Error: Failed to import data, count is zero."
  exit 1
fi
# 3. 执行SQL
$hive -e "$sto_sql"

echo "End syncing import and export sto data into DWD layer on $sync_year $sync_date .................."