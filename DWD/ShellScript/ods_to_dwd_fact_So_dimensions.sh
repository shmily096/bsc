#!/bin/bash
# Function:
#   sync up so dn data from ods to dwd layer
# History:
# 2022-09-05    slc   v1.0 
# 参数
target_db_name='opsdw' # 数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
echo "start syncing so dn data into DWD layer on ${sync_date} .................."
# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;

insert overwrite table ${target_db_name}.dwd_fact_So_dimensions partition(dt)
   select
		distinct 
		so_no ,
		order_type,
		order_reason,
		order_status,
		pick_up_plant,
		customer_code,
		division_id,
		customer_level3,
		customer_type,
		default_location,
		item_type,
		created_datetime,
		chinese_socreatedt,
		dt
    from ${target_db_name}.dwd_fact_sales_order_info
    where dt in (select distinct date_format(so_create_dt,'yyyy-MM-dd')xx
				from ${target_db_name}.ods_sales_order --源表:TRANS_SalesOrder
				where dt='$sync_date' and substr(so_create_dt,1,1)='2');"
# 2. 执行SQL
echo "$so_sql"
$hive -e "$so_sql"
echo "End syncing Sales order data into DWD layer on ${sync_date}.................."