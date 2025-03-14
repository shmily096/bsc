#!/bin/bash
# Function:
#   sync up dwt_dutycost_inoutbound_mr
# History:
# 2021-11-26 Amanda   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)
year_month=$(date  +'%Y-%m')

echo "start syncing dwt_dutycost_inoutbound_mr data into DWT layer on ${sync_date[month]} : ${sync_date[year]}"

# billed:
# dned:

# 1 Hive SQL string
dwt_sql="
use ${target_db_name};

set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
set hive.exec.parallel=false;

insert overwrite table dwt_dutycost_inoutbound_mr partition(dt_year, dt_month)
select 
    storage_location
    ,distribution_properties
    ,sap_upl_level4_name
    ,sap_upl_level5_name
    ,equiment_or_not
    ,division_display_name
    ,sum(act_payment) as act_payment
    ,dt_year
    ,dt_month
from dws_dutycost_inoutbound 
group by storage_location
    ,distribution_properties
    ,sap_upl_level4_name
    ,sap_upl_level5_name
    ,equiment_or_not
    ,division_display_name
    ,dt_year
    ,dt_month
;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_dsr_topic data into DWT layer on ${sync_date[month]} : ${sync_date[year]}"
