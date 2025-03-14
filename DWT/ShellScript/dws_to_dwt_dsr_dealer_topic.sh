#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-07-21    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date -d '-1 day' +%F)
fi
sync_year=${sync_date:0:4}
mm=${sync_date:5:2}
echo "start syncing data into dws layer on $sync_year :$mm .................."

sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;

insert overwrite table ${target_db_name}.dwt_dsr_dealer_topic partition(dt_year,dt_month)
select combined_dpq.division
      ,combined_dpq.sub_buname
      ,combined_dpq.customer_code
      ,combined_dpq.cust_level3
      ,combined_dpq.bill_month 
      ,combined_dpq.bill_year
      ,combined_dpq.quarter
      ,combined_dpq.dealer_name
      ,combined_dpq.parent_dealer_name
      ,max(combined_dpq.dealer_mon_complete) as dealer_mon_complete
      ,max(combined_dpq.dealer_mon_target) as dealer_mon_target
      ,max(combined_dpq.dealer_quater_target) as dealer_quater_target
      ,max(combined_dpq.dealer_quarter_target) as dealer_quarter_target
	  ,max(combined_dpq.bill_qty) as bill_qty
      ,'$sync_year' as dt_year
      ,cast(month('$sync_date') as VARCHAR(10)) as dt_month
 from (
SELECT division
    ,sub_division as sub_buname
    ,customer_code
    ,cust_level3
    ,bill_month 
    ,bill_year
    ,quarter
    ,dealer_name
    ,parent_dealer_name
    ,SUM(if(dealer_complete is null, 0, dealer_complete)) as dealer_mon_complete
    ,max(dealer_mon_target) as dealer_mon_target
    ,0 as dealer_quater_target
    ,max(dealer_mon_total_target) as dealer_quarter_target
	,sum(bill_qty) as bill_qty
    ,'$sync_year' as dt_year
    ,cast(month('$sync_date') as VARCHAR(10)) as dt_month
FROM dws_dsr_dealer_daily_transation
where dt_year='$sync_year'
and dt_month ='$mm'
group by division
    ,sub_division
    ,customer_code
    ,cust_level3
    ,bill_month
    ,bill_year
    ,quarter
    ,dealer_name
    ,parent_dealer_name
union all 
select division
    ,sub_buname
    ,sapi_d as customer_code
    ,dealer_type as cust_level3
    ,cast(month('$sync_date') as VARCHAR(10)) as bill_month 
    ,'$sync_year' as bill_year
    ,case when month('$sync_date') <=3 then '1'
          when month('$sync_date') <=6 and month('$sync_date') > 3 then '2'
          when month('$sync_date') <=9 and month('$sync_date') > 6 then '3'
          when month('$sync_date') <=12  then '4'
        end as quarter
    ,dealer_name
    ,parent_dealer_name
    ,0 as dealer_mon_complete
    ,case month('$sync_date')
       when 1 then month1_amount 
       when 2 then month2_amount 
       when 3 then month3_amount 
       when 4 then month4_amount 
       when 5 then month5_amount 
       when 6 then month6_amount 
       when 7 then month7_amount 
       when 8 then month8_amount 
       when 9 then month9_amount 
       when 10 then month10_amount 
       when 11 then month11_amount 
       when 12 then month12_amount  
     end as dealer_mon_target
    ,0 as dealer_quater_target
    ,case month('$sync_date')
       when 1 then q1_amount 
       when 2 then q1_amount 
       when 3 then q1_amount 
       when 4 then q2_amount 
       when 5 then q2_amount 
       when 6 then q2_amount 
       when 7 then q3_amount 
       when 8 then q3_amount 
       when 9 then q3_amount 
       when 10 then q4_amount 
       when 11 then q4_amount 
       when 12 then q4_amount  
     end as dealer_quarter_target
	 ,0 as bill_qty
    ,'$sync_year' as dt_year
    ,cast(month('$sync_date')as VARCHAR(10)) as dt_month
 from  (
 select fin_year
      , division
      , dealer_type
      , sub_buname
      , dealer_name
      , sapi_d
      , parent_dealer_name
      , month1_amount
      , month2_amount
      , month3_amount
      , month4_amount
      , month5_amount
      , month6_amount
      , month7_amount
      , month8_amount
      , month9_amount
      , month10_amount
      , month11_amount
      , month12_amount
      , q1_amount
      , q2_amount
      , q3_amount
      , q4_amount
   from dwd_fact_dealer_purchase_quotation --指标表
  where fin_year='$sync_year' 
    and dt in (select max(dt) from dwd_fact_dealer_purchase_quotation)
group by fin_year
      , division
      , dealer_type
      , sub_buname
      , dealer_name
      , sapi_d
      , parent_dealer_name
      , month1_amount
      , month2_amount
      , month3_amount
      , month4_amount
      , month5_amount
      , month6_amount
      , month7_amount
      , month8_amount
      , month9_amount
      , month10_amount
      , month11_amount
      , month12_amount
      , q1_amount
      , q2_amount
      , q3_amount
      , q4_amount ) q ) combined_dpq
group by combined_dpq.division
      ,combined_dpq.sub_buname
      ,combined_dpq.customer_code
      ,combined_dpq.cust_level3
      ,combined_dpq.bill_month 
      ,combined_dpq.bill_year
      ,combined_dpq.quarter
      ,combined_dpq.dealer_name
      ,combined_dpq.parent_dealer_name

"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  $sync_year :$mm  .................."