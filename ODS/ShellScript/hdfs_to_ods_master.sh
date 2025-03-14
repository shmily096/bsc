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

exchange_rate="
--1 Load data into table ods_exchange_rate from hdfs /bsc/origin_data/bsc_app_ops/exchange_rate
load data inpath '/bsc/origin_data/$origin_db_name/exchange_rate/$sync_date' overwrite 
into table ${target_db_name}.ods_exchange_rate 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/exchange_rate/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$exchange_rate"
fi

idd_master="
--2 Load data into table ods_idd_master from hdfs /bsc/origin_data/bsc_app_ops/IDD
load data inpath '/bsc/origin_data/$origin_db_name/IDD/$sync_date' overwrite 
into table ${target_db_name}.ods_idd_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/IDD/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$idd_master"
fi

calendar="
--3 Load data into table ods_calendar_master from /bsc/origin_data/bsc_app_ops/calendar
load data inpath '/bsc/origin_data/$origin_db_name/calendar/$sync_date' overwrite 
into table ${target_db_name}.ods_calendar_master;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/calendar/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$calendar"
fi

location="
--4 Load data into table ods_storage_location_master from /bsc/origin_data/bsc_app_ops/location
load data inpath '/bsc/origin_data/$origin_db_name/location/$sync_date' overwrite 
into table ${target_db_name}.ods_storage_location_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/location/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$location"
fi

plant="
--5 Load data into table ods_plant_master from /bsc/origin_data/bsc_app_ops/plant
load data inpath '/bsc/origin_data/$origin_db_name/plant/$sync_date' overwrite 
into table ${target_db_name}.ods_plant_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/plant/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$plant"
fi

material="
--6 Load data into table ods_material_master from /bsc/origin_data/bsc_app_ops/material
load data inpath '/bsc/origin_data/$origin_db_name/material/$sync_date' overwrite 
into table ${target_db_name}.ods_material_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/material/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$material"
fi

batch="
--7 Load data into table ods_batch_master from /bsc/origin_data/bsc_app_ops/batch
load data inpath '/bsc/origin_data/$origin_db_name/batch/$sync_date' overwrite 
into table ${target_db_name}.ods_batch_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/batch/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$batch"
fi

customer="
-- 8 Load data into table ods_customer_master from /bsc/origin_data/bsc_app_ops/customer
load data inpath '/bsc/origin_data/$origin_db_name/customer/$sync_date' overwrite 
into table ${target_db_name}.ods_customer_master 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/customer/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$customer"
fi

division="
-- 9 Load data into table ods_division_master from /bsc/origin_data/bsc_app_ops/division
load data inpath '/bsc/origin_data/$origin_db_name/division/$sync_date' overwrite 
into table ${target_db_name}.ods_division_master 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/division/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$division"
fi

knb1="
-- 9 Load data into table ods_customermaster_knb1 from /bsc/origin_data/bsc_app_ops/knb1
load data inpath '/bsc/origin_data/$origin_db_name/knb1/$sync_date' overwrite 
into table ${target_db_name}.ods_customermaster_knb1
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/knb1/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$knb1"
fi

knvi="
-- 9 Load data into table ods_customermaster_knvi from /bsc/origin_data/bsc_app_ops/knvi
load data inpath '/bsc/origin_data/$origin_db_name/knvi/$sync_date' overwrite 
into table ${target_db_name}.ods_customermaster_knvi 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/knvi/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$knvi"
fi

knvv="
-- 9 Load data into table ods_customermaster_knvv from /bsc/origin_data/bsc_app_ops/knvv
load data inpath '/bsc/origin_data/$origin_db_name/knvv/$sync_date' overwrite 
into table ${target_db_name}.ods_customermaster_knvv 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/knvv/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$knvv"
fi

cfda="
-- 10 Load data into table ods_cfda from /bsc/origin_data/bsc_app_ops/cfda
load data inpath '/bsc/origin_data/$origin_db_name/cfda/$sync_date' overwrite 
into table ${target_db_name}.ods_cfda 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/cfda/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$cfda"
fi

cfda_upn="
-- 11 Load data into table ods_cfda_upn from /bsc/origin_data/bsc_app_ops/cfda_upn
load data inpath '/bsc/origin_data/$origin_db_name/cfda_upn/$sync_date' overwrite 
into table ${target_db_name}.ods_cfda_upn 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/cfda_upn/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$cfda_upn"
fi





# 2. 执行加载数据SQL
$hive -e "$master_sql"

echo "End loading master data on {$sync_date} .................."




