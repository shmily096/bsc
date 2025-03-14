#!/bin/bash
# Function:
#   load hdfs master data to ODS master talbe 
# demo:
# load data inpath '/bsc/origin_data/bsc_app_ops/sales_order/2020-05-07' overwrite
#    into table bscdw.ods_sales_order 
#    partition(dt='2020-05-07');
# History:
# 2021-05-11    Donny   v1.0    draft

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天 
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "Start loading master data on {$sync_date} .................."
#  1.主数据SQL string
master_sql=""

plant="
--5 Load data into table ods_plant_master from /bsc/origin_data/bsc_app_ops/plant
load data inpath '/bsc/origin_data/$origin_db_name/plant_sway/$sync_date' overwrite 
into table ${target_db_name}.ods_plant_master_sway 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/plant_sway/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$plant"
fi


# 2. 执行加载数据SQL
$hive -e "$master_sql"

echo "End loading master data on {$sync_date} .................."




