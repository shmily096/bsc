#!/bin/bash
# Function:
#   load local data to ODS,dwd transation table
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
sync_date=$(date +%F) 

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天 
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式

# $1 local data type:  inbound_raw, inventory_raw, outbound_raw
if [ -n "$1" ] ;then 
    data_type=$1
else
   echo 'please use local date type: inbound_raw, inventory_raw, outbound_raw'
   exit 1
fi

# $2: the hdfs path of data file
#echo "$2"

#if [ -f "$2" ] ;then 
#    data_file=$2
#else
#    echo "The $2 does not exist"
#    exit 1
#fi

echo "Start loading data on $1.................."
#  1.业务数据SQL

inbound_raw="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/DP_Inbound_Rawdata.xlsx.csv' overwrite
 into table ods_inbound_rawdata partition(dt='$sync_date');
"

inventory_raw="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/DP_Inventory_Rawdata.xlsx.csv' overwrite
 into table ods_inventory_rawdata partition(dt='$sync_date');
"

outbound_raw="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/DP_Outbound_Rawdata.xlsx.csv' overwrite
 into table ods_outbound_rawdata partition(dt='$sync_date');
"

TP_report="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/TP_Validation_Report.xlsx.csv' overwrite
 into table ods_TP_vaildation_report partition(dt='$sync_date');
"

add_on_list="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/add_on_duty_list.xlsx.csv' overwrite
 into table ods_add_on_duty_list partition(dt='$sync_date');
"

saving_duty_list="
use ${target_db_name};
load data local inpath '/root/bscflow/data/ftp/saving_duty_list.xlsx.csv' overwrite
 into table ods_saving_duty_list partition(dt='$sync_date');
"


case $1 in
    "inbound_raw")
        local_sql_str="$inbound_raw"
        ;;
    "inventory_raw")
        local_sql_str="$inventory_raw"
        ;;
    "outbound_raw")
        local_sql_str="$outbound_raw"
        ;;
    "TP_report")
        local_sql_str="$TP_report"
        ;;
    "add_on_list")
        local_sql_str="$add_on_list"
        ;;
    "saving_duty_list")
        local_sql_str="$saving_duty_list"
        ;;
    *)
        echo "Usage $0 {inbound_raw|inventory_raw|outbound_raw|}"
        ;;
esac
# 2. 执行加载数据SQL
$hive -e "$local_sql_str"

echo "End loading data on $1.................."