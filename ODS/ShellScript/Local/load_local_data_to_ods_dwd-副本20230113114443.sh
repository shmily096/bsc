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

# $1 local data type: customer_level, customer_business_type, operation_type, rebate_rate
if [ -n "$1" ] ;then 
    data_type=$1
else
   echo 'please use local date type: cust_level, cust_type, operation_type, rebate_rate, le_srr'
   exit 1
fi

# $2: the hdfs path of data file
echo "$2"

if [ -f "$2" ] ;then 
    data_file=$2
else
    echo "The $2 does not exist"
    exit 1
fi

echo "Start loading data on $1 on $2 .................."
#  1.业务数据SQL
cust_level="
use ${target_db_name};
--1 Load data into table ods_customer_level from local 
load data local inpath '$2' overwrite into table ods_customer_level;

-load data into dwd from ods 
insert overwrite table dwd_dim_customer_level
select level1_code
       ,level1_english_name
       ,level1_chinese_name
       ,level2_code
       ,level2_english_name
       ,level2_chinese_name
       ,level3_code
       ,level3_english_name
       ,level3_chinese_name
       ,level4_code
       ,level4_chinese_name
       ,business_category
from ods_customer_level;
"
cust_type="
use ${target_db_name};
--load data into ods from local file
load data local inpath '$2' overwrite into table ods_cust_business_type;

--load data into dwd from ods 
insert overwrite table dwd_dim_cust_business_type partition(dt='$sync_date')
select * from ods_cust_business_type;
"

operation_type="
use ${target_db_name};
--load data into ods from local file
load data local inpath '$2' overwrite into table ods_order_operation_type;

--load data into dwd from ods 
insert overwrite table dwd_dim_order_operation_type partition(dt='$sync_date')
select * from ods_order_operation_type;
"

rebate_rate="
use ${target_db_name};
--load data into ods from local file
--load data local inpath '$2' overwrite into table ods_division_rebate_rate;
load data local inpath '/bscflow/RebateRate.txt' overwrite into table ods_division_rebate_rate;

--load data into dwd from ods 
insert overwrite table dwd_dim_division_rebate_rate partition(dt='$sync_date')
select 
    id
    ,upper(division)
    ,cust_business_type
    ,sub_divison
    ,rate
    ,default_rate
from ods_division_rebate_rate;
"

le_srr="
use ${target_db_name};
load data local inpath '$2' overwrite into table ods_le_srr partition(dt='$sync_date');
insert overwrite table dwd_dim_le_srr partition(dt='$sync_date')
select 
    division,
    year,
    month,
    le_cny,
    le_usd,
    srr_cny,
    srr_usd,
    srr_version
from ods_le_srr
where dt = '$sync_date';
"

case $1 in
    "cust_level")
        local_sql_str="$cust_level"
        ;;
    "cust_type")
        local_sql_str="$cust_type"
        ;;
    "operation_type")
        local_sql_str="$operation_type"
        ;;
    "rebate_rate")
        local_sql_str="$rebate_rate"
        ;;
    "le_srr")
        local_sql_str="$le_srr"
        ;;
    *)
        echo "Usage $0 {cust_level|cust_type|operation_type|rebate_rate|le_srr|}"
        ;;
esac
# 2. 执行加载数据SQL
$hive -e "$local_sql_str"

echo "End loading data on $1 on $2 .................."