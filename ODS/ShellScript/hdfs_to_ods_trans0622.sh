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
if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
fi

echo "Start loading data on {$sync_date} .................."

#  1.业务数据SQL
#Load data into table ods_sales_delivery from hdfs sales_order_dn by partition
so_dn_sql="
load data inpath '/bsc/origin_data/$origin_db_name/sales_order_dn/$sync_date' overwrite 
into table ${target_db_name}.ods_sales_delivery 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/sales_order_dn/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_dn_sql"
fi

#--2 Load data into table ods_work_order from hdfs /bsc/origin_data/bsc_app_ops/work_order by partition
wo_sql="
load data inpath '/bsc/origin_data/$origin_db_name/work_order/$sync_date' overwrite 
into table ${target_db_name}.ods_work_order 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/work_order/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$wo_sql"
fi
#--3 Load data into table ods_inbound_outbound_dn_mapping from hdfs /bsc/origin_data/bsc_app_ops/inbound_outbound_dn_mapping/ by partition
io_dn_map_sql="
load data inpath '/bsc/origin_data/$origin_db_name/inbound_outbound_dn_mapping/$sync_date' overwrite 
into table ${target_db_name}.ods_inbound_outbound_dn_mapping 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/inbound_outbound_dn_mapping/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$io_dn_map_sql"
fi

po_sql="
--4 Load data into table from hdfs /bsc/origin_data/bsc_app_ops/purchase_order
load data inpath '/bsc/origin_data/$origin_db_name/purchase_order/$sync_date' overwrite 
into table ${target_db_name}.ods_purchase_order 
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/purchase_order/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$po_sql"
fi

so_sql="
--5 Load data into table ods_sales_order from hdfs /bsc/origin_data/bsc_app_ops/sales_order
load data inpath '/bsc/origin_data/$origin_db_name/sales_order/$sync_date' overwrite
into table ${target_db_name}.ods_sales_order
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/sales_order/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_sql"
fi

ie_sto_sql="
--6 Load data into table ods_import_export_transaction from  /bsc/origin_data/bsc_app_ops/import_export_sto
load data inpath '/bsc/origin_data/$origin_db_name/import_export_sto/$sync_date' overwrite
into table ${target_db_name}.ods_import_export_transaction
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/import_export_sto/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$ie_sto_sql"
fi

ie_sto_dn="
--7 Load data into table ods_import_export_delivery from /bsc/origin_data/bsc_app_ops/import_export_sto_dn
load data inpath '/bsc/origin_data/$origin_db_name/import_export_sto_dn/$sync_date' overwrite
into table ${target_db_name}.ods_import_export_delivery
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/import_export_sto_dn/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$ie_sto_dn"
fi

in_dn_map_sql="
--8 Load data into table ods_commercial_invoice_dn_mapping from /bsc/origin_data/bsc_app_ops/commercial_invoice_dn_mapping
load data inpath '/bsc/origin_data/$origin_db_name/commercial_invoice_dn_mapping/$sync_date' overwrite
into table ${target_db_name}.ods_commercial_invoice_dn_mapping
partition(dt='$sync_date');"


hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/commercial_invoice_dn_mapping/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$in_dn_map_sql"
fi


dom_sto_sql="
--9 Load data into table ods_domestic_sto from /bsc/origin_data/bsc_app_ops/domestic_sto
load data inpath '/bsc/origin_data/$origin_db_name/domestic_sto/$sync_date' overwrite
into table ${target_db_name}.ods_domestic_sto
partition(dt='$sync_date');"

dom_sto_dn_sql="
--10 Load data into table ods_domestic_delivery from /bsc/origin_data/bsc_app_ops/domestic_sto_dn
load data inpath '/bsc/origin_data/$origin_db_name/domestic_sto_dn/$sync_date' overwrite
into table ${target_db_name}.ods_domestic_delivery
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/domestic_sto/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$dom_sto_sql"
fi


hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/domestic_sto_dn/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$dom_sto_dn_sql"
fi

# SO invoice
so_invoice_sql="
load data inpath '/bsc/origin_data/$origin_db_name/so_invoice/$sync_date' overwrite
into table ${target_db_name}.ods_so_invoice
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_invoice/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_invoice_sql"
fi

# Putaway information
putawy_sql="
load data inpath '/bsc/origin_data/$origin_db_name/putaway/$sync_date' overwrite
into table ${target_db_name}.ods_putaway_info
partition(dt='$sync_date');
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/putaway/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$putawy_sql"
fi

# ods_inventory_movement_trans
mov_sql="
load data inpath '/bsc/origin_data/$origin_db_name/movement/$sync_date' overwrite
into table ${target_db_name}.ods_inventory_movement_trans
partition(dt='$sync_date');
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/movement/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$mov_sql"
fi

onhand_sql="
load data inpath '/bsc/origin_data/$origin_db_name/inventory_onhand/$sync_date' overwrite
into table ${target_db_name}.ods_inventory_onhand
partition(dt='$sync_date');
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/inventory_onhand/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$onhand_sql"
fi

dpq_sql="
load data inpath '/bsc/origin_data/$origin_db_name/dealer_purchase_quotation/$sync_date' overwrite
into table ${target_db_name}.ods_dealer_purchase_quotation
partition(dt='$sync_date');
"

socd_sql="
load data inpath '/bsc/origin_data/$origin_db_name/so_receiving_confirmation/$sync_date' overwrite
into table ${target_db_name}.ods_so_dn_receiving_confirmation
partition(dt='$sync_date');
"

# Dealer purchase quotation
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dealer_purchase_quotation/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$dpq_sql"
fi

# so_receiving_confirmation
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_receiving_confirmation/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$socd_sql"
fi

# ods_salesorder_createdinfo
so_createinfo_sql="
-- Load data into table ods_salesorder_createdinfo from hdfs /bsc/origin_data/bsc_app_ops/so_createinfo
load data inpath '/bsc/origin_data/$origin_db_name/so_createinfo/$sync_date' overwrite
into table ${target_db_name}.ods_salesorder_createdinfo
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_createinfo/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_createinfo_sql"
fi

# ods_salesorder_partner
so_partner_sql="
-- Load data into table ods_salesorder_partner from hdfs /bsc/origin_data/bsc_app_ops/so_partner
load data inpath '/bsc/origin_data/$origin_db_name/so_partner/$sync_date' overwrite
into table ${target_db_name}.ods_salesorder_partner
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_partner/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_partner_sql"
fi

# ods_salesorder_text
so_text_sql="
-- Load data into table ods_salesorder_text from hdfs /bsc/origin_data/bsc_app_ops/so_text
load data inpath '/bsc/origin_data/$origin_db_name/so_text/$sync_date' overwrite
into table ${target_db_name}.ods_salesorder_text
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/so_text/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$so_text_sql"
fi



#TRANS_RRSalesForecast
RRSalesForecast_sql="
-- Load data into table ods_RRSalesForecast from hdfs /bsc/origin_data/bsc_app_ops/RRSalesForecast
load data inpath '/bsc/origin_data/$origin_db_name/RRSalesForecast/$sync_date' overwrite
into table ${target_db_name}.ods_RRSalesForecast
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/RRSalesForecast/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$RRSalesForecast_sql"
fi

#TRANS_DutybyUPN
DutybyUPN_sql="
-- Load data into table ods_DutybyUPN from hdfs /bsc/origin_data/bsc_app_ops/DutybyUPN
load data inpath '/bsc/origin_data/$origin_db_name/DutybyUPN/$sync_date' overwrite
into table ${target_db_name}.ods_DutybyUPN
partition(dt='$sync_date');"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/DutybyUPN/$sync_date"
if [ $? -eq 0 ]
then
    trans_sql="$trans_sql""$DutybyUPN_sql"
fi
#MDM_DealerMaster

MDM_DealerMaster_sql="
load data inpath '/bsc/origin_data/$origin_db_name/MDM_DealerMaster/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_dealermaster
partition(dt='$sync_date');"

# 2. 执行加载数据SQL
if [ "$1"x = "MDM_DealerMaster"x ];then
	echo "$1 $MDM_DealerMaster_sql"
	$hive -e "$MDM_DealerMaster_sql"
	echo "$1 finish"
else
    $hive -e "$trans_sql"
	echo "End loading data on {$sync_date} .................."

fi  










