#!bin/bash
# Function:
#   load hdfs wo qr to ODS transation table
# demo:
# load data inpath '/bsc/origin_data/bsc_app_ops/sales_order/2020-05-07' overwrite
# History:
# 2021-07-01    Donny   v1.0    init
# 2021-07-29    Donny   v1.1    add partition

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径

if [ -n "$1" ]; then
       sync_date=$1
else
       sync_date=$(date  +%F)
fi

load_sql_str="
use ${target_db_name};
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
---这个表放在ods_to_dwd_master后面跑,没有单独上调度
--load data into dwd from ods 
insert overwrite table dwd_dsr_topic_nonvalue
SELECT 
    '' as dsr_month,
    '' as dsr_year, 
    display_name as division, 
    0 as net_billed,
    0 as billed_rebate,
    0 as net_dned, 
    0 as dn_rebate, 
    0 as net_cr_dned,
    0 as net_cr,
    0 as net_fulfill, 
    0 as fulfill_rebate
FROM opsdw.dwd_dim_division
where dt='$sync_date' and id not in ('36','37','100');
"
$hive -e "$load_sql_str"
