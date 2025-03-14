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
origin_db_name='tableaudb' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天 
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
    #sync_date='2024-10-21'
	year_month=$(date  +'%Y-%m')
fi

echo "Start loading master data on {$sync_date} .................."
#  1.主数据SQL string
master_sql=""

csgn="
load data inpath '/bsc/origin_data/$origin_db_name/ods_csgn_stock/$sync_date' overwrite 
into table ${target_db_name}.ods_csgn_stock 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/ods_csgn_stock/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$csgn"
fi


confirmedReason="
load data inpath '/bsc/origin_data/$origin_db_name/ods_perfectorder_confirmedreason/$sync_date' overwrite 
into table ${target_db_name}.ods_perfectorder_confirmedreason 
partition(updatemon='$year_month');"

controlList="
load data inpath '/bsc/origin_data/$origin_db_name/ods_perfectorder_controllist/$sync_date' overwrite 
into table ${target_db_name}.ods_perfectorder_controllist 
partition(updatemon='$year_month');"





# 2. 执行加载数据SQL
# $hive -e "$master_sql"

# 2. 执行加载数据SQL
if [ "$1"x = "confirmedReason"x ];then
	echo "$1 $confirmedReason"
	$hive -e "$confirmedReason"
	echo "$1 finish"
elif [ "$1"x = "controlList"x ];then
	echo "$1 $controlList"
	$hive -e "$controlList"
elif [ "$1"x = "perfect"x ];then
	echo "$1 $marc"
	$hive -e "$confirmedReason"
	$hive -e "$controlList"
	echo "$1 finish"
else
    $hive -e "$master_sql"
	echo "End loading data on {$sync_date} .................."

fi  

echo "End loading master data on {$sync_date} .................."




