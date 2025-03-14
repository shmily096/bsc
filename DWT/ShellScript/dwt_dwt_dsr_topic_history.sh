#!/bin/bash
# Function:
#   sync up dwt_imported_topic data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天

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

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."

dwt_sql="
use ${target_db_name};
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;


insert overwrite table dwt_dsr_topic_history partition(dt='$sync_date')
SELECT 
bill_month,
bill_year, 
division, 
net_amount_shiped, 
net_amount_dn, 
fulfilled_value, 
rebate_value, 
net_cr_shiped, 
estimate_index,
shiped_and_fulfill,
net_amount_shiped_usd,
net_amount_dn_usd,
fulfilled_value_usd,
rebate_value_usd, 
net_cr_shiped_usd,
estimate_index_usd,
shiped_and_fulfill_usd,
le_cny,
le_usd,
srr_cny,
srr_usd,
bill_qty,
available_qty,
pick_up_qty,
pick_up_qty_cr,
total_workday,
total_workdaybytoday, 
dt_year,
dt_month,
 (nvl(net_amount_shiped,0)+nvl(net_amount_dn,0)+nvl(fulfilled_value,0)-nvl(rebate_value,0)+nvl(net_cr_shiped,0)+nvl(srr_cny,0))/6.39/1000 as tableau_invoice
FROM opsdw.dwt_dsr_topic 
where dt_year='$yester_year' and  dt_month between cast('$q_s_mon' as int) and cast('$q_e_mon' as int)

"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_imported_topic data into DWT layer on $sync_year .................."