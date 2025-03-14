#!/bin/bash
# Function:
#   load hdfs transation data to ODS transation table
# History:


# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='tableaudb' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天 
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "Start loading data on {$sync_date} .................."


demo_sql="
load data inpath '/bsc/origin_data/$origin_db_name/demo/$sync_date' overwrite
into table ${target_db_name}.ods_demo
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/demo/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$demo_sql"
fi

# 2. 执行加载数据SQL
$hive -e "$trans_sql"

echo "End loading data on {$sync_date} .................."