#!/bin/bash
# Function:
#   load hdfs transation data to ODS transation table
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

echo "Start loading data on {$1} .................."
#  1.业务数据SQL
trans_sql="
--1 Load data into table ods_customer_level from local 
load data local inpath '/user/code/bsc/ODS/localdata/customer_level.txt' overwrite 
into table ${target_db_name}.ods_customer_level ;
"

# 2. 执行加载数据SQL
$hive -e "$trans_sql"

echo "End loading data on {$1} .................."