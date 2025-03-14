#!/bin/bash
# Function:
#   sync up dws_dsr_cr_daily 
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



echo "start syncing data into dws layer on $sync_year :${sync_date[month]} .................."

# cr_dd:在invoice中取本月cr billed的so单号 且net >0
# divi:division


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


drop table if exists tmp_dws_dsr_cr_daily_cr_dd;
create table tmp_dws_dsr_cr_daily_cr_dd stored as orc as
    select  sales_id
           ,to_date(chinese_BillDate) as bill_date
           ,material
           ,sum(net_amount) as net_amount
		   ,division as division_display_name
           ,case when (lower(material)) like 'rbt%' then 1 else 0 end as upn_del_flag
           ,bill_type
		   ,sum(case when division in ('LT','LTS') and sales_type = 'DR' then 0 else bill_qty end) as bill_qty
		   ,sold_to
    from dwd_fact_sales_order_invoice
    where (lower(purchase_order) like '%cr' or lower(purchase_order) like '%pro')
    and net_amount>0
    and dt>=date_add('$sync_date',-35)
    and to_date(chinese_BillDate)>=date_add('$sync_date',-34)
	and case when division in ('LT','LTS') and sales_type not in ('OR','DR') then 1 else 0 end <> 1  ---LT:OR/DR
	group by 
			sales_id
           ,to_date(chinese_BillDate) 
           ,material
		   ,division 
           ,case when (lower(material)) like 'rbt%' then 1 else 0 end 
           ,bill_type
		   ,sold_to;

drop table if exists tmp_dws_dsr_cr_daily_reb;
create table tmp_dws_dsr_cr_daily_reb stored as orc as
    select  soinv.material 
        ,soinv.sales_id as so_no 
        ,soinf.rebate_rate 
        ,soinf.customer_code
        ,soinf.order_reason
    from 
    (
        select material 
            ,sales_id 
            ,sold_to 
            ,purchase_order
        from dwd_fact_sales_order_invoice
        where dt>=date_add('$sync_date',-35)
        and (lower(substring(purchase_order , 1, 5)) != 'price' 
        and ( sold_to not in ('554263', '107352')) 
        and lower(material) not in ('pd525', 'pd275', 'pd875') 
        and substr(lower(material), 1, 3)!='rbt')  
        group by
                material 
            ,sales_id 
            ,sold_to 
            ,purchase_order
    )soinv
    left join (
        select *   
          from dwd_fact_sales_order_info
         where dt>=date_add('$sync_date',-35)) soinf
    on soinv.material = soinf.material and soinv.sales_id = soinf.so_no 
    group by
        soinv.material 
        ,soinv.sales_id 
        ,soinf.rebate_rate 
        ,soinf.customer_code
        ,soinf.order_reason
;
drop table if exists tmp_dws_dsr_cr_daily_cust;
create table tmp_dws_dsr_cr_daily_cust stored as orc as
    select cust_account
          ,type_of_business
          ,case when (lower(cust_account)) like 'pd%' or (upper(type_of_business)) ='INTER-SA-SRR' then 1
          else 0
          end as cust_del_flag
          from dwd_dim_customer
          where  dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$sync_date',7))
          group by 
             cust_account
            ,type_of_business
;
drop table if exists tmp_dws_dsr_cr_daily_new_reb;
create table tmp_dws_dsr_cr_daily_new_reb stored as orc as
select  reb.material 
        ,reb.so_no 
        ,reb.customer_code
        ,reb.order_reason
        ,cust.cust_del_flag
    from tmp_dws_dsr_cr_daily_reb reb 
    left join tmp_dws_dsr_cr_daily_cust cust
        on reb.customer_code = cust.cust_account   
    group by
        reb.material 
        ,reb.so_no 
        ,reb.customer_code
        ,reb.order_reason
        ,cust.cust_del_flag
    ;



insert overwrite table dws_dsr_cr_daily partition(dt)
select  ww.sales_id
       ,ww.bill_date
       ,ww.material
       ,sum(ww.net_amount)                as net_cr
       ,ww.division_display_name
       ,ww.upn_del_flag
       ,ww.cust_del_flag
       ,case when ww.division_display_name = 'URO' and ww.order_reason='W18' then 1 else 0 end as orderreason_del_flag
       ,case when ww.division_display_name = 'ENDO' and ww.bill_type='ZIVR' then 1 else 0 end as billtype_del_flag
       ,date_format(ww.bill_date,'yyyy') as dt_year 
       ,date_format(ww.bill_date,'MM')   as dt_month
	   ,sum(ww.bill_qty)as cr_qty
	   ,ww.sold_to 		as customer_code
       ,ww.bill_date 	as dt
from 
(
     select  cr_dd.sales_id
            ,cr_dd.bill_date
            ,cr_dd.material
            ,cr_dd.net_amount
            ,cr_dd.bill_type
			,cr_dd.bill_qty
			,cr_dd.sold_to
			,cr_dd.division_display_name
			,cr_dd.upn_del_flag
            ,r.cust_del_flag
            ,r.order_reason
    from tmp_dws_dsr_cr_daily_cr_dd cr_dd
    left join tmp_dws_dsr_cr_daily_new_reb  r on cr_dd.sales_id = r.so_no and cr_dd.material = r.material
)ww
group by ww.sales_id
       , ww.bill_date
       , ww.material
       , ww.upn_del_flag
       , ww.division_display_name
       , ww.cust_del_flag
       , ww.bill_type
       , ww.order_reason
       , date_format(ww.bill_date,'yyyy')
       , date_format(ww.bill_date,'MM')
       , ww.sold_to
;
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  $sync_year :${sync_date[month]}  .................."