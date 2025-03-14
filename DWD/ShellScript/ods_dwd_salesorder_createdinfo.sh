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

echo "start syncing dwd_dim_le_srr data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
main_sql="
use ${target_db_name};
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.dynamic.partitions.pernode=200000;
set hive.exec.max.dynamic.partitions=200000;
-- cfda master data

insert overwrite table opsdw.dwd_salesorder_createdinfo partition(dt_month)
SELECT 
	so_no,
	TO_DATE(request_delivery_date) as request_delivery_date ,
	TO_DATE(so_create_dt)as so_create_dt,
	so_create_by,
	date_format(so_create_dt,'yyyy-MM')as dt_month
FROM opsdw.ods_salesorder_createdinfo
where dt='$sync_date' and  so_create_dt is not null;
"

# 2. 执行加载数据SQL
echo "$main_sql"
$hive -e "$main_sql"

echo "End syncing dwd_dim_le_srr data into DWD layer on ${sync_date} .................."

