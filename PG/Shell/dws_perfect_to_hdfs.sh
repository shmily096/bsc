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
    #默认取昨天的日期
    yester_day=$(date -d '-1 day' +%F)
fi
yester_year=${yester_day:0:4}
yester_month=${yester_day:0:7}-01
this_month=`date -d "${yester_day}" +%Y-%m-01`
this_year=`date -d "${yester_day}" +%Y-01-01`
sync_date=$(date  +%F)

echo "End loading master data on $q_s_mon:$q_e_mon:$yester_month:$yester_day .................."


# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_billed_daily'
export_dira='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_billed_daily data into HDFS on $yester_day .................."

# 1 Hive SQL string


dws_dq_exception_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dq_exception'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dq_exception where dt>'2024-08-18';
"

dwd_perfect_detail_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_perfect_detail'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_perfect_detail;
"


# 2. 执行加载数据SQL
if [ "$1"x = "aab"x ];then
	echo "hdfs $1 only run"	
    echo "$str_sql"
	$hive -e "$str_sql"
	echo "hdfs  finish dws_dsr_billed_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "perfect"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_perfect_detail_sql"
	$hive -e "$dwd_perfect_detail_sql"
	echo "hdfs  finish dwd_perfect_detail data into hdfs layer on ${sync_date} .................."
else
	echo "$1 not found"
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting ads_forwarder_app data into HDFS on $yester_day .................."


