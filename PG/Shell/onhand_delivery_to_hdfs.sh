#!/bin/bash
# Function:
#   将查询的结果导出至HDFS上
# History:
# 2021-11-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间 
if [ -n "$2" ] ;then 
    yester_day=$2
else
    yester_day=$(date -d '-1 day' +%F)
fi

# 2 设置导出的 HDFS路径

echo "start exporting $1 data into HDFS on $yester_day .................."

# 1 Hive SQL string
dn_detail_sql="
insert OVERWRITE directory '/bsc/opsdw/export/dwd_fact_sales_order_dn_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT so_no, delivery_id, line_number, material, qty, batch, qr_code, plant, chinese_dncreatedt, dt
FROM opsdw.dwd_fact_sales_order_dn_detail
where dt>=date_add('$yester_day',-1);
"
dn_info_sql="
insert OVERWRITE directory '/bsc/opsdw/export/dwd_fact_sales_order_dn_info'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT so_no, delivery_id, created_datetime, updated_datetime, created_by, updated_by, ship_to_address, real_shipto_address, planned_gi_date, actual_gi_date, receiving_confirmation_date, delivery_mode, carrier_id, pick_location_id, total_qty, plant, chinese_dncreatedt, dt
FROM opsdw.dwd_fact_sales_order_dn_info
where dt>=date_add('$yester_day',-1);
"
onhand_sql="
insert OVERWRITE directory '/bsc/opsdw/export/dwd_fact_inventory_onhand'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT trans_date, inventory_type, plant, storage_loc, profic_center, material, batch, quantity, unrestricted, inspection, blocked_material, expiration_date, standard_cost, extended_cost, update_date, dt
FROM opsdw.dwd_fact_inventory_onhand
where dt='$yester_day';
"
domestic_sto_dn_info_sql="
insert OVERWRITE directory '/bsc/opsdw/export/dwd_fact_domestic_sto_dn_info'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT sto_no, delivery_no, reference_dn_number, create_datetime, create_by, update_datetime, update_by, delivery_mode, dn_status, ship_from_location, ship_from_plant, ship_to_plant, ship_to_location, carrier, actual_migo_date, planned_good_issue_datetime, actua_good_issue_datetime, total_qty, actual_putaway_datetime, pgi_datetime, chinese_dncreatedt, dt
FROM opsdw.dwd_fact_domestic_sto_dn_info
where dt>=date_add('$yester_day',-1);
"
domestic_sto_dn_detail_sql="
insert OVERWRITE directory '/bsc/opsdw/export/dwd_fact_domestic_sto_dn_detail'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT sto_no, delivery_no, line_number, material, qty, batch, qr_code, chinese_dncreatedt, dt
FROM opsdw.dwd_fact_domestic_sto_dn_detail
where dt>=date_add('$yester_day',-1);
"
# 2. 执行加载数据SQL
if [ "$1"x = "dwd_fact_sales_order_dn_detail"x ];then
    echo "hdfs $1 only run"	
    echo "$dn_detail_sql"
    $hive -e "$dn_detail_sql"
    echo "hdfs  finish dwd_fact_sales_order_dn_detail data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dwd_fact_sales_order_dn_info"x ];then
    echo " hdfs $1 only run"
    echo "$dn_info_sql"
    $hive -e "$dn_info_sql"
    echo "hdfs  finish dwd_fact_sales_order_dn_info data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_fact_inventory_onhand"x ];then
    echo " hdfs $1 only run"
    echo "$onhand_sql"
    $hive -e "$onhand_sql"
    echo "hdfs  finish dwd_fact_inventory_onhand data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_fact_domestic_sto_dn_info"x ];then
    echo " hdfs $1 only run"
    echo "$domestic_sto_dn_info_sql"
    $hive -e "$domestic_sto_dn_info_sql"
    echo "hdfs  finish dwd_fact_domestic_sto_dn_info data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_fact_domestic_sto_dn_detail"x ];then
    echo " hdfs $1 only run"
    echo "$domestic_sto_dn_detail_sql"
    $hive -e "$domestic_sto_dn_detail_sql"
    echo "hdfs  finish dwd_fact_domestic_sto_dn_detail data into hdfs layer on ${sync_date} .................."
else
    echo "$1 not found"
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi
echo "End exporting onhand,delivery  data into HDFS on $yester_day .................."