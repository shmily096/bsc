#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-07-21    Donny   v1.0    init
# 2021-07-28    Winter  v1.1    update

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
dwd_fact_dealer_purchase_quotation_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_fact_dealer_purchase_quotation | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing data into dws layer on $sync_year :${sync_date[month]}: ${sync_date[day]}.:$mm................."

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

--with parts
With b as 
(select bd.so_no
    ,bd.net_billed
    ,bd.bill_date
    ,bd.material
    ,bd.billed_rebate
    ,bd.division
    ,bd.sub_division 
	,bd.bill_qty
    ,case when bd.customer_code is not null and bd.customer_code <>'' then bd.customer_code 
        else bb.customer_code 
      end as customer_code
    ,bb.customer_level3
 from dws_dsr_billed_daily bd 
 left join 
       (select so_no
              ,customer_code
              ,customer_level3
          from dwd_fact_sales_order_info so 
         where dt>=date_add('${sync_date[day]}',-100)
        group by so_no
                ,customer_code
                ,customer_level3)bb on bb.so_no =bd.so_no  
 where  bd.dt>=date_add('${sync_date[day]}',-35)--15
   and bd.upn_del_flag =0
   and bd.cust_del_flag =0
   and bd.OrderReason_del_flag =0
   and bd.BillType_del_flag =0
    )
,e AS 
(select b.bill_date
      , b.division
      , case when coalesce(dc.level3_code, b.customer_level3)='LP' then mat.lp_subbu_name
          else mat.subbu_name end as sub_division
      , customer_code
      , coalesce(dc.level3_code, b.customer_level3) as customer_level3
      , sum(net_billed) net_billed
	  , sum(bill_qty) bill_qty
 from b 
  left join ( select cust_account
                    ,level3_code
                from dwd_dim_customer
               where dt='$dwd_dim_customer_maxdt'
               ---in (select max(dt) from dwd_dim_customer)
            group by cust_account
                    ,level3_code) dc on b.customer_code = dc.cust_account
 left join (
             select material_code
                  , subbu_name
                  , lp_subbu_name
               from dwd_dim_material
             where dt='$dwd_dim_material_maxdt'
             ---in (select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
             ) mat on b.material=mat.material_code
 group by b.bill_date
         ,b.division
         ,case when coalesce(dc.level3_code, b.customer_level3)='LP' then mat.lp_subbu_name
            else mat.subbu_name end
         ,b.customer_code
         ,coalesce(dc.level3_code, b.customer_level3)
)
,dpq as 
(select fin_year
    ,division
    ,sub_buname
    ,dealer_name
    ,dealer_type
    ,sapi_d
    ,parent_dealer_name
    ,sum(month1_amount) month1_amount
    ,sum(month2_amount) month2_amount
    ,sum(month3_amount) month3_amount
    ,sum(month4_amount) month4_amount
    ,sum(month5_amount) month5_amount
    ,sum(month6_amount) month6_amount
    ,sum(month7_amount) month7_amount
    ,sum(month8_amount) month8_amount
    ,sum(month9_amount) month9_amount
    ,sum(month10_amount) month10_amount
    ,sum(month11_amount) month11_amount
    ,sum(month12_amount) month12_amount
    ,sum(q1_amount) as q1_amount
    ,sum(q2_amount) as q2_amount
    ,sum(q3_amount) as q3_amount
    ,sum(q4_amount) as q4_amount
from (
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
   from dwd_fact_dealer_purchase_quotation      
  where fin_year='$sync_year' 
    and dt='$dwd_fact_dealer_purchase_quotation_maxdt'
    --in (select max(dt) from dwd_fact_dealer_purchase_quotation)
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
      , q4_amount ) q
group by fin_year
       , division
       , dealer_type
       , sub_buname
       , dealer_name
       , sapi_d
       , parent_dealer_name
)

insert overwrite table ${target_db_name}.dws_dsr_dealer_daily_transation partition(dt)
select  f.division
       ,f.sub_division 
       ,f.customer_code 
       ,f.customer_level3 
       ,month(f.bill_date)                    as bill_month 
       ,f.bill_year 
       ,f.bill_date
       ,f.quarter
       ,f.dealer                              as dealer_name 
       ,f.parent_dealer_name 
       ,f.dealer_complete 
       ,f.dealer_mon_target
       ,f.dealer_mon_total_target
       ,date_format(f.bill_date,'yyyy') as      dt_year
       ,date_format(f.bill_date,'MM') as        dt_month
	   ,f.bill_qty
       ,f.bill_date as dt
from 
(
    select e.net_billed           as dealer_complete
		,e.bill_qty
        ,e.bill_date            as bill_date
        ,year(e.bill_date)      as bill_year
        ,e.customer_code
        ,e.division
        ,e.sub_division
        ,coalesce(dpq.dealer_type,e.customer_level3) as customer_level3
        ,case when dpq.dealer_name is not null 
            and dpq.dealer_name<>'' then  dpq.dealer_name       
            else ddc.cust_name 
        end as dealer
        ,dpq.parent_dealer_name as parent_dealer_name
        ,case month(e.bill_date) 
            when 1 then dpq.month1_amount 
            when 2 then dpq.month2_amount 
            when 3 then dpq.month3_amount 
            when 4 then dpq.month4_amount 
            when 5 then dpq.month5_amount 
            when 6 then dpq.month6_amount 
            when 7 then dpq.month7_amount 
            when 8 then dpq.month8_amount 
            when 9 then dpq.month9_amount 
            when 10 then dpq.month10_amount 
            when 11 then dpq.month11_amount 
            when 12 then dpq.month12_amount 
          else 0.0 
         end as dealer_mon_target
        ,case month(e.bill_date) 
            when 1 then dpq.q1_amount 
            when 2 then dpq.q1_amount 
            when 3 then dpq.q1_amount 
            when 4 then dpq.q2_amount 
            when 5 then dpq.q2_amount 
            when 6 then dpq.q2_amount 
            when 7 then dpq.q3_amount 
            when 8 then dpq.q3_amount 
            when 9 then dpq.q3_amount 
            when 10 then dpq.q4_amount 
            when 11 then dpq.q4_amount 
            when 12 then dpq.q4_amount 
          else 0.0 
         end as dealer_mon_total_target
        ,case when month(e.bill_date) <=3 then '1'
              when month(e.bill_date) <=6 and month(e.bill_date) > 3 then '2'
              when month(e.bill_date) <=9 and month(e.bill_date) > 6 then '3'
              when month(e.bill_date) <=12  then '4'
           else 0.0
          end as quarter       
     from e
    left join dpq
    on  
        year(e.bill_date) = dpq.fin_year 
        and lower(e.division)= lower(dpq.division) 
        and lower(e.sub_division)=lower(dpq.sub_buname)
        and e.customer_code = dpq.sapi_d
        
    left join
    (select cust_account
           ,cust_name
     FROM dwd_dim_customer
     where dt='$dwd_dim_customer_maxdt'
     --in (select max(dt) from dwd_dim_customer)
     group by cust_account
             ,cust_name )ddc on ddc.cust_account= e.customer_code
     --where e.net_billed > 0  
) f
where f.bill_year='$sync_year'
--group by
 --      f.division
--       ,f.sub_division 
 --      ,f.customer_code 
--       ,f.customer_level3 
 --      ,month(f.bill_date)
 --      ,f.bill_year 
 --      ,f.bill_date
 --      ,f.quarter
 --      ,f.dealer
 --      ,f.parent_dealer_name 
  --     ,f.dealer_complete 
--       ,f.dealer_mon_target
--       ,0.0
 --      ,date_format(f.bill_date,'yyyy')
 --      ,date_format(f.bill_date,'MM')
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  $sync_year :${sync_date[month]}  .................."