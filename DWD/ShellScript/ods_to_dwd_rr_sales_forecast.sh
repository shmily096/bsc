#!/bin/bash
# Function:
#   sync up sales order data from ods to dwd layer
# History:
# 2021-11-16    Amanda   v1.0    init


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

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi

echo "start syncing so into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table dwd_rr_sales_forecast partition(dt)
select 
      Part
      ,SuperDivision
      ,Division2
      ,Division
      ,ProcurementSegment
      ,ProductGroup
      ,Family
      ,Type
      ,Part1
      ,Month1
      ,Month2
      ,Month3
      ,Month4
      ,Month5
      ,Month6
      ,Month7
      ,Month8
      ,Month9
      ,Month10
      ,Month11
      ,Month12
      ,Month13
      ,Month14
      ,Month15
      ,Month16
      ,Month17
      ,Month18
      ,Month19
      ,Month20
      ,Month21
      ,Month22
      ,Month23
      ,Month24
      ,ForcastVersion
      ,UpdateDate
      ,ForcastCycle
      ,PLANT
      ,date_format(ForcastVersion,'yyyy-MM-dd')
from ods_RRSalesForecast --源表:TRANS_RRSalesForecast
where dt='$sync_date'and substr(ForcastVersion,1,1)='2'
"
# 2. 执行SQL
$hive -e "$so_sql"
# 3. 执行SQL，并判断dwd查询结果是否为空
count=`$hive -e "select count(*) from ods_RRSalesForecast where dt='$sync_date'and substr(ForcastVersion,1,1)='2'" | tail -n1`
if [ $count -eq 0 ]; then
  echo " 今天ods 没有数据"
else
    count_dwd=`$hive -e "select count(*) from dwd_rr_sales_forecast where dt=(select max(date_format(ForcastVersion,'yyyy-MM-dd')) from ods_RRSalesForecast where dt='$sync_date' )" | tail -n1`
    if [ $count_dwd -eq 0 ]; then
    echo "Error: Failed to import data, count is zero."
    exit 1
    fi
fi
echo "End syncing Sales order data into DWD layer on ${sync_date} .................."