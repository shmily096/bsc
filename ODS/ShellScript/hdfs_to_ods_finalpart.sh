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

echo "Start loading data on {$sync_date} .................."


inbound_tracking_sql="
--11 Load data into table ods_shipment_status_inbound_tracking from /bsc/origin_data/bsc_app_ops/inbound_tracking
load data inpath '/bsc/origin_data/$origin_db_name/inbound_tracking/$sync_date' overwrite
into table ${target_db_name}.ods_shipment_status_inbound_tracking
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/inbound_tracking/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$inbound_tracking_sql"
fi


CTMCustomsPermissionCertification_sql="
-- 13 Load data into table ods_CTMCustomsPermissionCertification from /bsc/origin_data/bsc_app_ops/CTMCustomsPermissionCertification
load data inpath '/bsc/origin_data/$origin_db_name/CTMCustomsPermissionCertification/$sync_date' overwrite 
into table ${target_db_name}.ods_CTM_CPC 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/CTMCustomsPermissionCertification/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$CTMCustomsPermissionCertification_sql"
fi

CTMCustomerMaster_sql="
-- 12 Load data into table ods_CTMCustomerMaster from /bsc/origin_data/bsc_app_ops/CTMCustomerMaster
load data inpath '/bsc/origin_data/$origin_db_name/CTMCustomerMaster/$sync_date' overwrite 
into table ${target_db_name}.ods_CTM_Customer_Master 
partition(dt='$sync_date');
"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/CTMCustomerMaster/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$CTMCustomerMaster_sql"
fi

#TRANS_CTMIntegrationQuery
ctm_integration_query_sql="
-- Load data into table ods_CTMIntegrationQuery from hdfs /bsc/origin_data/bsc_app_ops/CTMIntegrationQuery
load data inpath '/bsc/origin_data/$origin_db_name/CTMIntegrationQuery/$sync_date' overwrite
into table ${target_db_name}.ods_ctm_intergrationquery
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/CTMIntegrationQuery/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$ctm_integration_query_sql"
fi



# 2. 执行加载数据SQL
$hive -e "$trans_sql"

echo "End loading data on {$sync_date} .................."