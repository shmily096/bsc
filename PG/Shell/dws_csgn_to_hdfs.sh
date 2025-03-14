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

# 获取3个月前的日期（减上3个月）  
month_date3=$(date -d '-30 day' +%F)

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
dwd_trans_csgn_clear_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_trans_csgn_clear' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_trans_csgn_t2_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_trans_csgn_t2' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_trans_salesdealerinventory_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_trans_salesdealerinventory' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_hk_expiration_maxdt=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_hk_expiration' | tail -n 1 | awk -F'=' '{print $NF}'`

# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_billed_daily'
export_dira='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_billed_daily data into HDFS on $yester_day .................."

# 1 Hive SQL string
str_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dir}'
row format delimited fields terminated by '\t' 
stored as textfile
select 
so_no, net_billed, bill_date, material, billed_rebate, division, sub_division, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, customer_code, dt_year, dt_month, bill_qty, dt
from opsdw.dws_dsr_billed_daily 
--where dt between '$q_s_date' and '$q_e_date'
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon'
;
"
cr_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dira}'
row format delimited fields terminated by '\t' 
stored as textfile
SELECT so_no, bill_date, material, net_cr, division_display_name, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, dt_year, dt_month, cr_qty, customer_code, dt
FROM opsdw.dws_dsr_cr_daily
--where dt>=date_add('$yester_day',-31)
where dt_year='$yester_year' and  dt_month between '$q_s_mon'and '$q_e_mon';
"
dwd_trans_csgn_clear_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_csgn_clear'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_trans_csgn_clear
where dt='$dwd_trans_csgn_clear_maxdt' and updatedt>='$month_date3';
"

dwd_trans_csgn_t2_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_csgn_t2'
row format delimited fields terminated by '\001' 
stored as textfile
select 
id, updatedt, active, dealercode, dealername, dealertype, nbr, salesdate, divisionid, divisionname, 
upn, batch, qrcode, qty, unitprice, standardcostusd, standardcost, '波科物寄售' consignmenttype, ordertype, 
parentdealercode, parentdealername, dt 
from opsdw.dwd_trans_csgn_t2
where dt='$dwd_trans_csgn_t2_maxdt' or day(dt)=1;
"


dwd_trans_salesdealerinventory_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_salesdealerinventory'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_trans_salesdealerinventory
where year_mon >'2024-05';
"

dwd_trans_consignmenttracking_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_consignmenttracking'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_trans_consignmenttracking
where year_mon >'2024-05';
"

dwd_csgnturn_over_rate_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_csgnturn_over_rate'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_csgnturn_over_rate
where year_mon >'2024-05';
"

dwd_trans_consignmentlist_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_trans_consignmentlist'
row format delimited fields terminated by '\001' 
stored as textfile
select * 
from opsdw.dwd_trans_consignmentlist
where year_mon >'2024-05';
"

dws_fact_sales_order_invoice_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_fact_sales_order_invoice'
row format delimited fields terminated by '\001' 
stored as textfile
select bill_date ,delivery_no ,material ,sales_line_no ,batch ,
bill_qty ,net_amount ,tax_amount ,sales_type  ,division ,dt
from opsdw.dwd_fact_sales_order_invoice where dt>='2024-01-01'
and sales_type in ('OR','KE');
"

dwd_otds_itemlevel_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_otds_itemlevel'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT vbeln, posnr, matnr, division, customer_req_date, year_month, 
pardate, year_month_date, bo_reason
FROM opsdw.dwd_otds_itemlevel;
"

dwd_hk_expiration_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_hk_expiration'
row format delimited fields terminated by '\001' 
stored as textfile
SELECT * FROM opsdw.dwd_hk_expiration
where dt='$dwd_hk_expiration_maxdt';
"


# 2. 执行加载数据SQL
if [ "$1"x = "aab"x ];then
	echo "hdfs $1 only run"	
    echo "$str_sql"
	$hive -e "$str_sql"
	echo "hdfs  finish dws_dsr_billed_daily data into hdfs layer on ${yester_day} .................."
elif [ "$1"x = "csgn_clear"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_trans_csgn_clear_sql"
	$hive -e "$dwd_trans_csgn_clear_sql"
    $hive -e "$dwd_trans_csgn_t2_sql"
    $hive -e "$dwd_trans_salesdealerinventory_sql"
    $hive -e "$dwd_trans_consignmenttracking_sql"
    $hive -e "$dwd_csgnturn_over_rate_sql"
    $hive -e "$dwd_trans_consignmentlist_sql"
    $hive -e "$dws_fact_sales_order_invoice_sql"   
    $hive -e "$dwd_otds_itemlevel_sql"
    $hive -e "$dwd_hk_expiration_sql"
	echo "hdfs  finish dwd_trans_csgn_clear data into hdfs layer on ${dwd_trans_csgn_clear_maxdt} .................."
elif [ "$1"x = "otds_itemlevel"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_otds_itemlevel_sql"
	$hive -e "$dwd_otds_itemlevel_sql"
	echo "hdfs  finish dwd_trans_csgn_t2 data into hdfs layer on today .................."
elif [ "$1"x = "clear"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_trans_csgn_t2_sql"
	$hive -e "$dwd_trans_csgn_t2_sql"
	echo "hdfs  finish dwd_trans_csgn_t2_sql data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."
elif [ "$1"x = "tracking"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_trans_consignmenttracking_sql"
	$hive -e "$dwd_trans_consignmenttracking_sql"
	echo "hdfs  finish dwd_trans_consignmenttracking_sql data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."
elif [ "$1"x = "turnover"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_csgnturn_over_rate_sql"
	$hive -e "$dwd_csgnturn_over_rate_sql"
	echo "hdfs  finish dwd_csgnturn_over_rate data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."
elif [ "$1"x = "list"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_trans_consignmentlist_sql"
	$hive -e "$dwd_trans_consignmentlist_sql"
	echo "hdfs  finish dwd_trans_consignmentlist data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."
elif [ "$1"x = "invoice"x ];then
    echo " hdfs $1 only run"
	echo "$dws_fact_sales_order_invoice_sql"
	$hive -e "$dws_fact_sales_order_invoice_sql"
	echo "hdfs  finish dws_fact_sales_order_invoice data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."
elif [ "$1"x = "hk"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_hk_expiration_sql"
	$hive -e "$dwd_hk_expiration_sql"
	echo "hdfs  finish dwd_hk_expiration data into hdfs layer on ${dwd_trans_salesdealerinventory_maxdt} .................."

else
	echo "$1 not found"
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting ads_forwarder_app data into HDFS on $yester_day .................."