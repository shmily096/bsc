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

sync_date=$(date  +%F)
sync_year=$(date  +'%Y')
this_month=`date -d "${sync_date}" +%Y-%m-01`
this_year=`date -d "${sync_date}" +%Y-01-01`

# 2 设置导出的 HDFS路径
export_dir='/bsc/opsdw/export/dws_dsr_billed_daily'
export_dira='/bsc/opsdw/export/dws_dsr_cr_daily'

echo "start exporting dws_dsr_billed_daily data into HDFS on $sync_date .................."

# 1 Hive SQL string
str_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dir}'
row format delimited fields terminated by '\t' 
stored as textfile
select a.*
from opsdw.dws_dsr_billed_daily a
where dt>=date_add('$sync_date',-31);
"
cr_sql="
use ${target_db_name};
insert OVERWRITE directory '${export_dira}'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_cr_daily
where dt>=date_add('$sync_date',-31);
"
dwd_dim_customer_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_customer'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_dim_customer
where dt='$sync_date';
"
dwd_dim_material_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwd_dim_material'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwd_dim_material
where dt='$sync_date';
"
dws_dsr_dned_daily_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_dned_daily'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_dned_daily
where dt_year>='$sync_year';
"
dws_dsr_fulfill_daily_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_fulfill_daily'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_fulfill_daily
WHERE dt_year >='$sync_year'and dt_month >='04'
;
"
dws_dsr_dealer_daily_transation_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_dsr_dealer_daily_transation'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_dsr_dealer_daily_transation
where dt>=date_add('$sync_date',-31);
"
#目前是全表更新后续可改为按月更新 先推4月以后的
dwt_dsr_dealer_topic_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_dsr_dealer_topic'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwt_dsr_dealer_topic
where dt_year>='$sync_year' and  dt_month>= '4'
;
"
#目前是全表更新后续可改为按月更新 先推4月以后的
dwt_dsr_topic_sql="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dwt_dsr_topic'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dwt_dsr_topic
where dt_year>='$sync_year' and  dt_month>= '4'
;
"
#推最近3个月的数据
dws_kpi_sales_waybill_timi="
use ${target_db_name};
insert OVERWRITE directory '/bsc/opsdw/export/dws_kpi_sales_waybill_timi'
row format delimited fields terminated by '\t' 
stored as textfile
select * 
from opsdw.dws_kpi_sales_waybill_timi
where dt_year>='$sync_year' and  dt_month>=month(add_months('$this_month',-3))
;
"
# 2. 执行加载数据SQL
if [ "$1"x = "dws_dsr_billed_daily"x ];then
	echo "hdfs $1 only run"	
    echo "$str_sql"
	$hive -e "$str_sql"
	echo "hdfs  finish dws_dsr_billed_daily data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_dsr_cr_daily"x ];then
    echo " hdfs $1 only run"
	echo "$cr_sql"
	$hive -e "$cr_sql"
	echo "hdfs  finish dws_dsr_cr_daily data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_dim_customer"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_customer_sql"
	$hive -e "$dwd_dim_customer_sql"
	echo "hdfs  finish dwd_dim_customer data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwd_dim_material"x ];then
    echo " hdfs $1 only run"
	echo "$dwd_dim_material_sql"
	$hive -e "$dwd_dim_material_sql"
	echo "hdfs  finish dwd_dim_material data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_dsr_dned_daily"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_dned_daily_sql"
	$hive -e "$dws_dsr_dned_daily_sql"
	echo "hdfs  finish dws_dsr_dned_daily_sql data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_dsr_fulfill_daily"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_fulfill_daily_sql"
	$hive -e "$dws_dsr_fulfill_daily_sql"
	echo "hdfs  finish dws_dsr_fulfill_daily data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_dsr_dealer_daily_transation"x ];then
    echo " hdfs $1 only run"
	echo "$dws_dsr_dealer_daily_transation_sql"
	$hive -e "$dws_dsr_dealer_daily_transation_sql"
	echo "hdfs  finish dws_dsr_dealer_daily_transation_sql data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_dsr_dealer_topic"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_dsr_dealer_topic_sql"
	$hive -e "$dwt_dsr_dealer_topic_sql"
	echo "hdfs  finish dwt_dsr_dealer_topic_sql data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dwt_dsr_topic"x ];then
    echo " hdfs $1 only run"
	echo "$dwt_dsr_topic_sql"
	$hive -e "$dwt_dsr_topic_sql"
	echo "hdfs  finish dwt_dsr_topic_sql data into hdfs layer on ${sync_date} .................."
elif [ "$1"x = "dws_kpi_sales_waybill_timi"x ];then
    echo " hdfs $1 only run"
	echo "$dws_kpi_sales_waybill_timi"
	$hive -e "$dws_kpi_sales_waybill_timi"
	echo "hdfs  finish dws_kpi_sales_waybill_timi data into hdfs layer on ${sync_date} .................."
else
    echo "hdfs  all run"
    echo "$str_sql"
    $hive -e "$str_sql"
	echo "dws_dsr_billed_daily finish"
    echo "$cr_sql"
	$hive -e "$cr_sql"
    echo "dws_dsr_cr_daily finish"
    echo "$dwd_dim_customer_sql"
	$hive -e "$dwd_dim_customer_sql"
    echo "dwd_dim_customer finish"
    echo "$dwd_dim_material_sql"
	$hive -e "$dwd_dim_material_sql"
    echo "dwd_dim_material_sql finish"
    echo "$dws_dsr_dned_daily_sql"
	$hive -e "$dws_dsr_dned_daily_sql"
    echo "dws_dsr_dned_daily_sql finish"
    echo "$dws_dsr_fulfill_daily_sql"
	$hive -e "$dws_dsr_fulfill_daily_sql"
    echo "dws_dsr_fulfill_daily_sql finish"
    echo "$dws_dsr_dealer_daily_transation_sql"
	$hive -e "$dws_dsr_dealer_daily_transation_sql"
    echo "--------------dws_dsr_dealer_daily_transation_sql finish"
    echo "$dwt_dsr_dealer_topic_sql"
	$hive -e "$dwt_dsr_dealer_topic_sql"
    echo "--------------dwt_dsr_dealer_topic_sql finish"	
 	echo "$dwt_dsr_topic_sql"
	$hive -e "$dwt_dsr_topic_sql"
    echo "--------------dwt_dsr_topic_sql finish"		
	echo "$dws_kpi_sales_waybill_timi"
	$hive -e "$dws_kpi_sales_waybill_timi"
	echo "--------------dws_kpi_sales_waybill_timi finish"	
fi
exitCode=$?
if [ "$exitCode" -ne 0 ];then  #ne不等于
    echo "[Error] hive execute failed!"
    exit $exitCode
fi

echo "End exporting ads_forwarder_app data into HDFS on $sync_date .................."