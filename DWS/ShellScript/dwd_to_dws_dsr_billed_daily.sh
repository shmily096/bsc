#!/bin/bash
# Function:
#   sync up dws_dsr_billed_daily
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

dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing data into dws layer on $sync_year :${sync_date[month]} .................."
#.a--在invoice中取本月非CRbilled的so单号
#.reb--取出返利
#divi-取division sub_division 
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
drop table if exists dws_dsr_billed_daily_a;
create table dws_dsr_billed_daily_a stored as orc as
    select  sales_id
           ,to_date(bill_date) as bill_date
           ,material
           ,sum(net_amount) as net_amount
           ,bill_type
           ,sales_type
           ,lower(purchase_order) as purchase_order
           ,bill_id
		   ,sum(case when division in ('LT','LTS') and sales_type = 'DR' then 0 else bill_qty end) as bill_qty
    from dwd_fact_sales_order_invoice
    where (lower(purchase_order) not like '%cr' and lower(purchase_order) not like '%pro' )
      and upper(sold_to) NOT LIKE 'PD%'
      and lower(material) not like 'rbt%'
      and dt>=date_add('$sync_date',-35) ---16
      ---and to_date(chinese_BillDate)>=date_add('$sync_date',-34)--15
	  and sold_to not in ('554263', '107352','690326')  --srr  -- '690326':CD
	  and case when division in ('LT','LTS') and coalesce(sales_type,'empty') not in ('OR','DR') then 1 else 0 end <> 1  ---LT:OR/DR
	  group by sales_id
             , to_date(bill_date)
             , material           
             , bill_type
             ,sales_type
             , lower(purchase_order) 
             ,bill_id;

drop table if exists dws_dsr_billed_daily_reb;
create table dws_dsr_billed_daily_reb stored as orc as
    select  soinv.material 
        ,soinv.sales_id as  so_no
        ,soinv.rebate_rate
        ,nvl(soinf.customer_code,soinv.sold_to)as customer_code
        ,soinf.order_reason
        ,soinv.bill_id
    from 
    (
        select material 
            ,sales_id 
            ,sold_to 
            ,purchase_order
            ,rebate_rate
            ,bill_id
        from dwd_fact_sales_order_invoice
        where  dt>=date_add('$sync_date',-365)
        --dt>=date_add('$sync_date',-25)--16
        --and (lower(substring(purchase_order , 1, 5)) != 'price' 
        and( ( sold_to not in ('554263', '107352')) 
        --and lower(material) not in ('pd525', 'pd275', 'pd875') 
        and substr(lower(material), 1, 3)!='rbt')  
        group by
                material 
            ,sales_id 
            ,sold_to 
            ,purchase_order
            ,rebate_rate
            ,bill_id
    )soinv
    left join (select material,so_no,customer_code,order_reason 
    from dwd_fact_sales_order_info  
            where dt>=date_add('$sync_date',-365))soinf on soinv.material = soinf.material and soinv.sales_id = soinf.so_no 
    group by
            soinv.material 
        ,soinv.sales_id 
        ,soinv.rebate_rate 
        ,nvl(soinf.customer_code,soinv.sold_to)
        ,soinf.order_reason
        ,soinv.bill_id;

drop table if exists dws_dsr_billed_daily_divi;
create table dws_dsr_billed_daily_divi stored as orc as
    select material_code
        ,division_display_name
        ,sub_division
        ,case when (lower(material_code)) like 'rbt%' then 1
              when (lower(product_line1_code)) = '3201002' and  division_display_name='EP' then 1
          else 0 end as upn_del_flag
        ,product_line1_code
        ,upper(sap_upl_level4_name) as sap_upl_level4_name
    from dwd_dim_material 
    where dt ='$dwd_dim_material_maxdt'
    --in ( select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
    group by material_code
        ,division_display_name
        ,sub_division
        ,product_line1_code
        ,upper(sap_upl_level4_name);

drop table if exists dws_dsr_billed_daily_cust;
create table dws_dsr_billed_daily_cust stored as orc as
    select cust_account
          ,type_of_business
          ,case when (lower(cust_account)) like 'pd%' or (upper(type_of_business)) ='INTER-SA-SRR' then 1
          else 0
          end as cust_del_flag
      from dwd_dim_customer
     where 
     --dt='$dwd_dim_customer_maxdt' 
     dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$dwd_dim_customer_maxdt',7))
   group by cust_account
           ,type_of_business;
           
drop table if exists dws_dsr_billed_daily_new_reb;
create table dws_dsr_billed_daily_new_reb stored as orc as
select  reb.material 
        ,reb.so_no 
        ,reb.rebate_rate 
        ,reb.customer_code
        ,reb.order_reason
        ,cust.cust_del_flag
        ,reb.bill_id
    from dws_dsr_billed_daily_reb reb 
    left join dws_dsr_billed_daily_cust cust
        on reb.customer_code = cust.cust_account  
    ;

insert overwrite table ${target_db_name}.dws_dsr_billed_daily partition(dt)
select  ww.sales_id 
       ,net_billed
       ,ww.bill_date 
       ,ww.material 
       ,ww.rebate 
       ,divi.division_display_name
       ,divi.sub_division 
       ,divi.upn_del_flag
       ,ww.cust_del_flag
       ,case when divi.division_display_name = 'URO' and ww.order_reason='W18' then 1
          ----   when divi.division_display_name = 'EP'  and ww.purchase_order like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%LSPRO%' then 1    ---- 2023 修改该逻辑
          ----   when divi.division_display_name = 'EP'  and ww.purchase_order like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%RHY%' then 1
          ----   when divi.division_display_name = 'EP'  and ww.purchase_order like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%MICROPACE%' then 1
         else 0 end as orderreason_del_flag
       ,case when divi.division_display_name = 'ENDO' and ww.bill_type='ZIVR' then 1 else 0 end as billtype_del_flag
       ,ww.customer_code 
       ,date_format(ww.bill_date,'yyyy') as dt_year 
       ,date_format(ww.bill_date,'MM')   as dt_month
	   , bill_qty
       ,ww.sales_type
       ,ww.bill_date  as dt
from 
(
    select  a.sales_id 
           ,sum(a.net_amount)                  as net_billed 
           ,a.bill_date 
           ,a.material 
           ,sum (a.net_amount * r.rebate_rate) as rebate 
           ,r.customer_code
           ,r.cust_del_flag
           ,a.bill_type
           ,a.sales_type
           ,a.purchase_order
           ,r.order_reason
		   ,sum(a.bill_qty) as bill_qty
    from dws_dsr_billed_daily_a a
    left join dws_dsr_billed_daily_new_reb r  on a.sales_id = r.so_no and a.material = r.material and a.bill_id=r.bill_id
    group by  a.sales_id 
             ,a.bill_date 
             ,a.material 
             ,r.customer_code
             ,r.cust_del_flag
             ,a.bill_type
             ,a.sales_type
             ,a.purchase_order
             ,r.order_reason
			 ,a.bill_qty
)ww
left join dws_dsr_billed_daily_divi divi on ww.material = divi.material_code 
union all
    select so_no as sales_id 
          ,cast(net_billed as decimal(38,2))as net_billed
          ,cast(bill_date as date) as bill_date
          ,material
          ,cast(billed_rebate as decimal(28,2)) as rebate 
          ,divi.division_display_name 
          ,divi.sub_division 
          ,0 as upn_del_flag
          ,0 as cust_del_flag
          ,0 as orderreason_del_flag
          ,0 as billtype_del_flag
          ,customer_code 
          ,date_format(bill_date,'yyyy') as dt_year 
          ,date_format(bill_date,'MM')   as dt_month
	      ,cast(bill_qty as decimal(38,2))as bill_qty
          ,'' as sales_type
          ,cast(bill_date as date)  as dt
    from dwd_dim_offline_transaction transaction
    left join dws_dsr_billed_daily_divi divi on transaction.material = divi.material_code 
	where bill_date >=date_add('$sync_date',-35)


 
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  $sync_year :${sync_date[month]}  .................."