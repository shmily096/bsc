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
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_cr_daily data into HDFS on $sync_date .................."

# 1 Hive SQL string
str_sql="

use ${target_db_name};

insert OVERWRITE directory '${export_dir}'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_cr_daily
--where dt>=date_add('$sync_date',-32);

"

echo "$str_sql"
# 2. 执行加载数据SQL
$hive -e "$str_sql"

exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting dws_dsr_cr_daily data into HDFS on $sync_date .................."
