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

clear="
load data inpath '/bsc/origin_data/$origin_db_name/ods_clear_stock/$sync_date' overwrite 
into table ${target_db_name}.ods_clear_stock 
partition(dt='$sync_date');"

kpireport="
load data inpath '/bsc/origin_data/$origin_db_name/ods_kpi_report/$sync_date' overwrite 
into table ${target_db_name}.ods_kpi_report 
partition(dt='$sync_date');"

kpicategory="
load data inpath '/bsc/origin_data/$origin_db_name/ods_kpi_category/$sync_date' overwrite 
into table ${target_db_name}.ods_kpi_category 
partition(dt='$sync_date');"

kpicomplaint="
load data inpath '/bsc/origin_data/$origin_db_name/ods_kpi_complaint/$sync_date' overwrite 
into table ${target_db_name}.ods_kpi_complaint 
partition(dt='$sync_date');"

kpisupplyweek="
load data inpath '/bsc/origin_data/$origin_db_name/ods_otds_supply_week/$sync_date' overwrite 
into table ${target_db_name}.ods_otds_supply_week 
partition(dt='$sync_date');"

kpiotdsdemand="
load data inpath '/bsc/origin_data/$origin_db_name/ods_otds_demand/$sync_date' overwrite 
into table ${target_db_name}.ods_otds_demand 
partition(dt='$year_month');"

kpiotdsitemlevel="
load data inpath '/bsc/origin_data/$origin_db_name/ct_otds_itemlevel/$sync_date' overwrite 
into table ${target_db_name}.ct_otds_itemlevel 
partition(dt='$year_month');"

kpipodn="
load data inpath '/bsc/origin_data/$origin_db_name/ods_po_dn_cycletime/$sync_date' overwrite 
into table ${target_db_name}.ods_po_dn_cycletime 
partition(dt='$sync_date');"


# 2. 执行加载数据SQL
# $hive -e "$master_sql"

# 2. 执行加载数据SQL
if [ "$1"x = "clear"x ];then
	echo "$1 $clear"
	$hive -e "$clear"
	echo "$1 finish"
elif [ "$1"x = "detail"x ];then
	echo "$1 $detail"
	$hive -e "$detail"
	echo "$1 finish"
elif [ "$1"x = "kpi"x ];then
	echo "$1 $kpireport"
	$hive -e "$kpireport"
	# $hive -e "$kpicategory"
	#$hive -e "$kpicomplaint" 改为SQL server获取
	$hive -e "$kpipodn"
	echo "$1 finish"
elif [ "$1"x = "kpiotds"x ];then
	echo "$1 $kpiotdsdemand"
	$hive -e "$kpisupplyweek"
	$hive -e "$kpiotdsdemand"
	$hive -e "$kpiotdsitemlevel"
	echo "$1 finish"
else
    $hive -e "$master_sql"
	echo "End loading data on {$sync_date} .................."

fi  

echo "End loading master data on {$sync_date} .................."




