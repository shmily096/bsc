#!/bin/bash
# Function:
#   sync up xxx data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing xxxx data into DWT layer on  ${sync_date[month]} : ${sync_date[year]}.................."

# 1 Hive SQL string
dwt_sql="
use ${target_db_name};
insert overwrite table xxx
--select part


"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing xxx data into DWT layer on $sync_date .................."