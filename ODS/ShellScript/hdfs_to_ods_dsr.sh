#!/bin/bash
# Function:
#   load hdfs master data to ODS master talbe 
# demo:
# load data inpath '/bsc/origin_data/bsc_app_ops/sales_order/2020-05-07' overwrite
#    into table bscdw.ods_sales_order 
#    partition(dt='2020-05-07');
# History:
# 2022-06-23    slc   v1.0    draft

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天 
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$2" ] ;then 
    sync_date=$2
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
MDM_DealerMaster_sql="
load data inpath '/bsc/origin_data/$origin_db_name/MDM_DealerMaster/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_dealermaster
partition(dt='$sync_date');"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/MDM_DealerMaster/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$MDM_DealerMaster_sql"
fi
dpq_sql="
load data inpath '/bsc/origin_data/$origin_db_name/dealer_purchase_quotation/$sync_date' overwrite
into table ${target_db_name}.ods_dealer_purchase_quotation
partition(dt='$sync_date');
"
# Dealer purchase quotation
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dealer_purchase_quotation/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dpq_sql"
fi

so_dn_sql="
load data inpath '/bsc/origin_data/$origin_db_name/sales_order_dn/$sync_date' overwrite 
into table ${target_db_name}.ods_sales_delivery 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/sales_order_dn/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$so_dn_sql"
fi

socd_sql="
load data inpath '/bsc/origin_data/$origin_db_name/so_receiving_confirmation/$sync_date' overwrite
into table ${target_db_name}.ods_so_dn_receiving_confirmation
partition(dt='$sync_date');
"
# so_receiving_confirmation
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_receiving_confirmation/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$socd_sql"
fi

so_invoice_sql="
load data inpath '/bsc/origin_data/$origin_db_name/so_invoice/$sync_date' overwrite
into table ${target_db_name}.ods_so_invoice
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_invoice/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$so_invoice_sql"
fi

so_sql="
--5 Load data into table ods_sales_order from hdfs /bsc/origin_data/bsc_app_ops/sales_order
load data inpath '/bsc/origin_data/$origin_db_name/sales_order/$sync_date' overwrite
into table ${target_db_name}.ods_sales_order
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/sales_order/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$so_sql"
fi

onhand_sql="
load data inpath '/bsc/origin_data/$origin_db_name/inventory_onhand/$sync_date' overwrite
into table ${target_db_name}.ods_inventory_onhand
partition(dt='$sync_date');
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/inventory_onhand/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$onhand_sql"
fi

so_createinfo_sql="
-- Load data into table ods_salesorder_createdinfo from hdfs /bsc/origin_data/bsc_app_ops/so_createinfo
load data inpath '/bsc/origin_data/$origin_db_name/so_createinfo/$sync_date' overwrite
into table ${target_db_name}.ods_salesorder_createdinfo
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_createinfo/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$so_createinfo_sql"
fi
TRANS_FS10NDetail_sql="
load data inpath '/bsc/origin_data/$origin_db_name/TRANS_FS10NDetail/$sync_date' overwrite
into table ${target_db_name}.ods_trans_fs10ndetail;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/TRANS_FS10NDetail/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$TRANS_FS10NDetail_sql"
fi
TRANS_FS10N_sql="
load data inpath '/bsc/origin_data/$origin_db_name/TRANS_FS10N/$sync_date' overwrite
into table ${target_db_name}.ods_trans_fs10n;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/TRANS_FS10N/$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$TRANS_FS10N_sql"
fi
# 2. 执行加载数据SQL

if [ "$1"x = "ods_exchange_rate"x ];then
	echo "$1 $exchange_rate"
	$hive -e "$exchange_rate"
	echo "$1 finish"
elif [ "$1"x = "MDM_DealerMaster"x ];then
	echo "$1 $MDM_DealerMaster_sql"
	$hive -e "$MDM_DealerMaster_sql"
	echo "$1 finish"
elif [ "$1"x = "TRANS_FS10NDetail"x ];then
	echo "$1 $TRANS_FS10NDetail_sql"
	$hive -e "$TRANS_FS10NDetail_sql"
	echo "$1 finish"
elif [ "$1"x = "TRANS_FS10N"x ];then
	echo "$1 $TRANS_FS10N_sql"
	$hive -e "$TRANS_FS10N_sql"
	echo "$1 finish"
elif [ "$1"x = "ods_material_master"x ];then   
	echo "$1 $material"
	$hive -e "$material"
	echo "$1 finish"
elif [ "$1"x = "ods_customer_master"x ];then   
	echo "$1 $customer"
	$hive -e "$customer"
	echo "$1 finish"
elif [ "$1"x = "ods_division_master"x ];then   
	echo "$1 $division"
	$hive -e "$division"
	echo "$1 finish"
elif [ "$1"x = "ods_customermaster_knb1"x ];then   
	echo "$1 $knb1"
	$hive -e "$knb1"
	echo "$1 finish"
elif [ "$1"x = "ods_dealer_purchase_quotation"x ];then   
	echo "$1 $dpq_sql"
	$hive -e "$dpq_sql"
	echo "$1 finish"
elif [ "$1"x = "ods_customermaster_knvi"x ];then   
	echo "$1 $knvi"
	$hive -e "$knvi"
	echo "$1 finish"
elif [ "$1"x = "ods_sales_delivery"x ];then   
	echo "$1 $so_dn_sql"
	$hive -e "$so_dn_sql"
	echo "$1 finish"
elif [ "$1"x = "ods_so_dn_receiving_confirmation"x ];then   
	echo "$1 $socd_sql"
	$hive -e "$socd_sql"
	echo "$1 finish"	
elif [ "$1"x = "ods_so_invoice"x ];then   
	echo "$1 $so_invoice_sql"
	$hive -e "$so_invoice_sql"
	echo "$1 finish"	
elif [ "$1"x = "ods_sales_order"x ];then   
	echo "$1 $so_sql"
	$hive -e "$so_sql"
	echo "$1 finish"	
elif [ "$1"x = "ods_inventory_onhand"x ];then   
	echo "$1 $onhand_sql"
	$hive -e "$onhand_sql"
	echo "$1 finish"	
elif [ "$1"x = "ods_salesorder_createdinfo"x ];then   
	echo "$1 $so_createinfo_sql"
	$hive -e "$so_createinfo_sql"
	echo "$1 finish"		
else
    $hive -e "$master_sql"
	echo "End loading dsr data on {$sync_date} .................."

fi  



