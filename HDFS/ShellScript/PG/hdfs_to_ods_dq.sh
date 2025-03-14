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


marc="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_marc/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_marc 
partition(dt='$sync_date');"

maex_bsc="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_maex_bsc/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_maex_bsc 
partition(dt='$sync_date');"

maex_crm="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_maex_crm/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_maex_crm 
partition(dt='$sync_date');"

mara="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_mara/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_mara 
partition(dt='$sync_date');"

mlan="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_mlan/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_mlan 
partition(dt='$sync_date');"

mvke="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_mvke/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_mvke 
partition(dt='$sync_date');"

zv001="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_zv001/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_zv001 
partition(dt='$sync_date');"

zv002="
load data inpath '/bsc/origin_data/$origin_db_name/ods_dq_zv002/$sync_date' overwrite 
into table ${target_db_name}.ods_dq_zv002 
partition(dt='$sync_date');"

adh_mara="
load data inpath '/bsc/origin_data/$origin_db_name/ods_adh_mara/$sync_date' overwrite 
into table ${target_db_name}.ods_adh_mara 
partition(yearmon='$year_month');"


# 2. 执行加载数据SQL
# $hive -e "$master_sql"

# 2. 执行加载数据SQL
if [ "$1"x = "mara"x ];then
	echo "$1 $mara"
	$hive -e "$mara"
	echo "$1 finish"
elif [ "$1"x = "crm"x ];then
	echo "$1 $marc"
	$hive -e "$maex_crm"
elif [ "$1"x = "adh_mara"x ];then
	echo "$1 $adh_mara"
	$hive -e "$adh_mara"
elif [ "$1"x = "dq"x ];then
	echo "$1 $marc"
	$hive -e "$marc"
	$hive -e "$maex_bsc"
	$hive -e "$maex_crm"
	$hive -e "$mara"
	$hive -e "$mlan"
	$hive -e "$mvke"
	$hive -e "$zv001"
	$hive -e "$zv002"
	$hive -e "$adh_mara"
	echo "$1 finish"
else
    $hive -e "$master_sql"
	echo "End loading data on {$sync_date} .................."

fi  

echo "End loading master data on {$sync_date} .................."




