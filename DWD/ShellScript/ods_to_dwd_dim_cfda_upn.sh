#!/bin/bash
# Function:
#   sync up dwd_dim_cfda_upn data to dwd layer
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

echo "start syncing dwd_dim_cfda_upn data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
main_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- cfda master data
insert overwrite table ${target_db_name}.dwd_dim_cfda_upn partition(dt='$sync_date')
select distinct
       registration_no        string
     , upn                    string
     , valid_fromdate         string
     , valid_enddate          string
from ${target_db_name}.ods_cfda_upn
where dt='$sync_date' and substring(valid_fromdate,1,1)>0; 
"
# 2. 执行加载数据SQL
$hive -e "$main_sql"
#导入pg
sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwd_dim_cfda_upn
sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwd_dim_cfda_upn
echo "End syncing dwd_dim_cfda_upn data into DWD layer on ${sync_date} .................."