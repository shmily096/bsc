#!/bin/bash
# Function:
#   sync up master data from ods data to dwd layer
# History:
# 2021-05-12    Donny   v1.0    draft

# 设置必要的参数
target_db_name='opsdw'                # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive       # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 如果是输入的日期按照取输入日期，否则取当前时间的前一天
# 时间格式都配置成 YYYY-MM-DD 格式，这是 Hive 默认支持的时间格式
if [ -n "$2" ]; then
       sync_date=$2
else
       sync_date=$(date  +%F)
fi

echo "start syncing master data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
master_sql=""
set_sql="
-- 配置参数
--set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
"
dwd_dim_plant_sql="
--2 Plant
insert overwrite table ${target_db_name}.dwd_dim_plant partition(dt='$sync_date')
select  plant_code
        ,name1
        ,nvl(name2, name1)
        ,postl_code
        ,city
        ,search_term1
        ,search_term2
from ${target_db_name}.ods_plant_master ---MDM_Plant 全量更新
where dt='$sync_date'
    and plant_code is not null;
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_plant_master/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_plant_sql"
fi
dwd_dim_locaiton_sql="
--3 Location
insert overwrite table ${target_db_name}.dwd_dim_locaiton partition(dt='$sync_date')
select d_plant
       ,plant_name
       ,location_id
       ,location_status
       ,storage_location
       ,storage_definition
from ${target_db_name}.ods_storage_location_master --MDM_StorageLocation 全量更新
where dt='$sync_date'
    and location_id is not null; 
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_storage_location_master/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_locaiton_sql"
fi
dwd_dim_batch_sql="
-- 4 batch
insert overwrite table ${target_db_name}.dwd_dim_batch partition(dt='$sync_date')
select material
       ,batch
       ,from_unixtime(unix_timestamp(shelf_life_exp_date, 'dd.MM.yyyy'),'yyyy-MM-dd')
       ,country_of_origin
       ,from_unixtime(unix_timestamp(date_of_manuf, 'yyyy.MM.dd'),'yyyy-MM-dd')
       ,cfda
from ${target_db_name}.ods_batch_master --MDM_BatchMaster全量更新
where dt='$sync_date'; 
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_batch_master/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_batch_sql"
fi
dwd_dim_calendar_sql="
--5 Calendar
insert overwrite table ${target_db_name}.dwd_dim_calendar
select cal_date
       ,cal_month
       ,cal_year
       ,cal_quarter
       ,weeknum_m1
       ,weeknum_y1
       ,weeknum_m2
       ,weeknum_y2
       ,weekday
       ,workday
       ,workday_flag
       ,po_pattern
       ,DATE_FORMAT(year_month_date, 'yyyy-MM-dd') 
       ,month_weeknum
       ,day_of_week
from ${target_db_name}.ods_calendar_master; 
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_calendar_master"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_calendar_sql"
fi
dwd_dim_exchange_rate_sql="
--6 Exchange Rate
insert overwrite table ${target_db_name}.dwd_dim_exchange_rate partition(dt='$sync_date')
select  from_currency
       ,to_currency
       ,valid_from
       ,rate
       ,ratio_from
       ,ratio_to
from ${target_db_name}.ods_exchange_rate
where dt='$sync_date'; 
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_exchange_rate/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_exchange_rate_sql"
fi
dwd_dim_division_sql="
--7 division
insert overwrite table ${target_db_name}.dwd_dim_division partition(dt='$sync_date')
select  id
       ,division
       ,short_name
       ,cn_name
       --,case when id='10' then 'PION' else display_name end as display_name
       ,display_name
       ,case display_name
             when 'PION' then 'product_line4_name'
             when 'PI' then 'product_line4_name'
             when 'IC' then 'product_line3_name'
             when 'URO' then 'product_line2_name'
             when 'PUL' then 'product_line2_name'
       else 'product_line1_name'
       end
from ${target_db_name}.ods_division_master
where dt='$sync_date'; 
"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_division_master/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_division_sql"
fi
dwd_dim_customer_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.auto.convert.join=false;

--8 customer
insert overwrite table ${target_db_name}.dwd_dim_customer partition(dt='$sync_date')
select  cust.cust_account
       ,cust.cust_name
       ,cust.cust_name2
       ,cust.city
       ,cust.post_code
       ,cust.rg
       ,cust.searchterm
       ,cust.street
       ,cust.telephone1
       ,cust.fax_number
       ,cust.tit
       ,cust.orblk
       ,cust.blb
       ,cust.cust_group
       ,cust.cl
       ,cust.dlv
       ,cust.del
       ,cust.cust_name3
       ,cust.cust_name4
       ,cust.distr
       ,cust.cust_b
       ,cust.transp_zone
       ,cust.country
       ,nvl(cust.delete_flag,'') as delete_flag
       ,nvl(cust.tfn,'') as tfn
       ,cust_level.level1_code -- level1
       ,cust_level.level1_english_name -- level1_name
       ,cust_level.level2_code -- level2
       ,cust_level.level2_english_name  -- level2 name
       ,cust_level.level3_code  -- level3
       ,cust_level.level3_english_name  -- level3 name
       ,cust_level.level4_code  -- level4
       ,cust_level.business_category -- Category
       ,cust.telebox_nr
       ,nvl(cust.payment_block,'') as payment_block
       ,cust.master_record
       ,cust.type_of_business
       ,cust.created_by
       ,cust.create_dt
       ,cust.customer_sales
       ,nvl(knb1.account_nr,'') as account_nr
       ,knb1.company_code
       ,knvi.tax_category
       ,knvi.tax_classification
       ,dealermaster.dealercode AS sapid
       ,dealermaster.dealername 
       ,dealermaster.dealertype  
       ,dealermaster.parentsapid
       ,dealermaster.parentdealername
       ,dealermaster.parentdealertype
       ,dealermaster.isactiveindms
from 
(
    select * from ${target_db_name}.ods_customer_master
       where dt='$sync_date'
           and country='CN'
) cust
left outer join  ${target_db_name}.ods_customer_level cust_level 
   on cust.type_of_business=cust_level.level4_code and cust.type_of_business is not null and cust_level.level4_code is not null
left outer join 
(    select
       cust_account
       ,account_nr
       ,company_code
    from ${target_db_name}.ods_customermaster_knb1 
    where dt='$sync_date'
)knb1
   on cust.cust_account = knb1.cust_account and cust.cust_account is not null
left outer join 
(    select
       cust_account
       ,tax_category
       ,tax_classification
    from ${target_db_name}.ods_customermaster_knvi
    where dt='$sync_date'
) knvi
   on cust.cust_account = knvi.cust_account and cust.cust_account is not null
left outer join
( select
       dealercode,
       dealername, 
       dealertype, 
       parentsapid,
       parentdealername,
       parentdealertype, 
       isactiveindms
from ${target_db_name}.ods_mdm_dealermaster
where dt='$sync_date'and dealertype is  not null 
)dealermaster 
       on cust.cust_account =dealermaster.dealercode and cust.cust_account is not null
;

"
hdfs dfs -test -d "/bsc/opsdw/ods/ods_customer_master/dt=$sync_date"
if [ $? -eq 0 ]
then
    master_sql="$master_sql""$dwd_dim_customer_sql"
fi





# 2. 执行加载数据SQL
if [ "$1"x = "dwd_dim_plant"x ];then
	echo "$1 $set_sql$dwd_dim_plant_sql"
	$hive -e "$set_sql""$dwd_dim_plant_sql"
	echo "$1 finish"
elif [ "$1"x = "dwd_dim_locaiton"x ];then   
	echo "$1 $dwd_dim_locaiton_sql"
	$hive -e "$set_sql""$dwd_dim_locaiton_sql"
	echo "$1 finish"	
elif [ "$1"x = "dwd_dim_batch"x ];then   
	echo "$1 $dwd_dim_batch_sql"
	$hive -e "$set_sql""$dwd_dim_batch_sql"
	echo "$1 finish"	
elif [ "$1"x = "dwd_dim_calendar"x ];then   
	echo "$1 $dwd_dim_calendar_sql"
	$hive -e "$set_sql""$dwd_dim_calendar_sql"
	echo "$1 finish"	
elif [ "$1"x = "dwd_dim_exchange_rate"x ];then   
	echo "$1 $dwd_dim_exchange_rate_sql"
	$hive -e "$set_sql""$dwd_dim_exchange_rate_sql"
	echo "$1 finish"	
elif [ "$1"x = "dwd_dim_division"x ];then   
	echo "$1 $dwd_dim_division_sql"
	$hive -e "$set_sql""$dwd_dim_division_sql"
	echo "$1 finish"
elif [ "$1"x = "dwd_dim_customer"x ];then   
	echo "$1 $dwd_dim_customer_sql"
	$hive -e "$set_sql""$dwd_dim_customer_sql"
	echo "$1 finish"
else
    $hive -e "$set_sql""$master_sql"
	echo "End loading dsr data on {$sync_date} .................."
    count_customer=`$hive -e "select count(*) from ${target_db_name}.dwd_dim_customer where dt='$sync_date'" | tail -n1`
    if [ $count_customer -eq 0 ]; then
    echo "Error: Failed to import data, count_customer is zero."
    exit 1
    fi
    echo "End syncing master data into DWD layer on ${sync_date} .................."

fi
