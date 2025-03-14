#!/bin/bash
# Function:
#   sync up dwd_dim_cfda data to dwd layer
# History:
# 2021-10-21    Donny   v1.0    init

export LANG="en_US.UTF-8"
# export LC_ALL=zh_CN.GB2312;
# export LANG=zh_CN.GBK
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dwd_dim_cfda data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
main_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- cfda master data
insert overwrite table ${target_db_name}.dwd_dim_cfda partition(dt='$sync_date')
select distinct
       registration_no
     , devision_id
     , upn_name
     , product_name_en
     , commerce_name
     , gm_kind
     , gm_catalog
     , gm_catelog_new
     , manufacturer
     , manufacturer_address
     , manufacturingsite_address
     , register_info
     , register_address
     , sterilization_mode
     , sterilization_validity
     , service_year
     , product_material
     , storage_conditions
     , storage_conditions_temperature string
     , transport_condition
     , transport_condition_temperature
     , valid_fromdate
     , valid_enddate
     , is_actived
     , created_date
     , last_modified_date
from ${target_db_name}.ods_cfda
where dt='$sync_date' ; 
"
# 2. 执行加载数据SQL
$hive -e "$main_sql"

echo "End syncing dwd_dim_cfda data into DWD layer on ${sync_date} .................."