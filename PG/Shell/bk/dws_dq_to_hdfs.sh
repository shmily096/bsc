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
mm=10#${yester_day:5:2}
if ((mm >= 1 ))&&((mm <= 3 ));then
    q_s_date=${yester_day:0:4}-01-01
    q_s_mon=01
    q_l_date=${yester_day:0:4}-02-01
    q_l_mon=02
    q_e_date=${yester_day:0:4}-03-31
    q_e_mon=03
elif ((mm >= 4 ))&&((mm <= 6 ));then
    q_s_date=${yester_day:0:4}-04-01
    q_s_mon=04
    q_l_date=${yester_day:0:4}-05-01
    q_l_mon=05
    q_e_date=${yester_day:0:4}-06-30
    q_e_mon=06
elif ((mm >= 7 ))&&((mm <= 9 ));then
    q_s_date=${yester_day:0:4}-07-01
    q_s_mon=07
    q_l_date=${yester_day:0:4}-08-01
    q_l_mon=08
    q_e_date=${yester_day:0:4}-09-30
    q_e_mon=09
elif ((mm >= 10 ))&&((mm <= 12 ));then
    q_s_date=${yester_day:0:4}-10-01
    q_s_mon=10
    q_l_date=${yester_day:0:4}-11-01
    q_l_mon=11
    q_e_date=${yester_day:0:4}-12-31
    q_e_mon=12
fi
echo "End loading master data on $q_s_mon:$q_e_mon:$yester_month:$yester_day .................."

dwd_dim_all_kpi_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_dim_all_kpi' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_outbound_distribution_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_outbound_distribution' | tail -n 1 | awk -F'=' '{print $NF}'`

# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_billed_daily'
export_dira='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_billed_daily data into HDFS on $yester_day .................."

# 1 Hive SQL string

dwd_trans_csgn_t2_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_csgn_t2'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_trans_csgn_t2
where dt='$dwd_trans_csgn_t2_maxdt' or day(dt)=1;
"


dws_dq_exception_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dq_exception'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dq_exception where dt>'2024-08-18';
"

dwd_dq_exception_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dq_exception'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_dq_exception where dt>'2024-08-18';
"


# 2. 执行加载数据SQL
if [ "$1"x = "aab"x ];then
	echo "hdfs $1 only run"	
    echo "$str_sql"
	$hive -e "$str_sql"
	echo "hdfs  finish dws_dsr_billed_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "dq"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dq_exception_sql"
	$hive -e "$dws_dq_exception_sql"
    $hive -e "$dwd_dq_exception_sql"
	echo "hdfs  finish dws_dq_exception data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dq_exception_sql"
	$hive -e "$dwd_dq_exception_sql"
	echo "hdfs  finish dwd_dq_exception data into hdfs layer on ${sync_date} .................."
else
	echo "$1 not found"
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting ads_forwarder_app data into HDFS on $yester_day .................."