
#!/bin/bash
# Function:
# 
# History:
# 2021-11-25    Amanda   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."
# 1 Hive SQL string
str_sql="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

insert overwrite table ads_lifecycle_leadtime_division_slcyh_topic partition(dt_year, dt_month)
select 
    pgi_year
    ,pgi_month
    ,division
    ,e2e
    ,in_store
    ,so_proce
    ,putaway_location   string
    ,dt_year
    ,dt_month
from dwt_lifecycle_leadtime_division_slcyh_topic
where dt_year='${sync_date[year]}'
      --and dt_month='${sync_date[month]}'
      ;
"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

echo "End syncing lifecycle leadtime of slc and yh ratio data data into ads layer on $sync_date .................."