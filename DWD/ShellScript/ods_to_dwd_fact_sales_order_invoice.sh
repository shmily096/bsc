#!/bin/bash
# Function:
#   sync up sales order invocie data from ods to dwd layer
# History:
# 2021-05-27    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
yesterday=$(date -d '-1 day' +%F)
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_division_rebate_rate_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_division_rebate_rate | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing so invoice data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
drop table if exists tmp_dwd_fact_sales_order_invoice_cust;
create table tmp_dwd_fact_sales_order_invoice_cust stored as orc as
    select distinct
        cust_account
        ,level3_code
        ,level4_code as customer_type
    from dwd_dim_customer  --源表:MDM_CustomerMaster,次表MDM_CustomerMaster_KNB1,MDM_CustomerMaster_KNVI,ods_customer_level
    where  dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$dwd_dim_customer_maxdt',7))
      ---='$dwd_dim_customer_maxdt'
;
drop table if exists tmp_dwd_fact_sales_order_invoice_sku;
create table tmp_dwd_fact_sales_order_invoice_sku stored as orc as
    select distinct
         bu_sku.material_code 
        ,bu_sku.division_id
        ,bu_sku.division_display_name
        ,bu_sku.default_location
        ,bu_sku.sub_division
        ,bu_sku.sub_division_bak
        ,bu_sku.business_group
    from dwd_dim_material bu_sku --源表主表MDM_MaterialMaster 全量更新,次表MDM_DivisionMaster 全量更新
    where  bu_sku.dt ='$dwd_dim_material_maxdt'
        ---in ( select max(dt) from dwd_dim_material max_dt where dt>=date_sub('$sync_date',10))
;
drop table if exists tmp_dwd_fact_sales_order_invoice_rebate;
create table tmp_dwd_fact_sales_order_invoice_rebate stored as orc as
    select * from dwd_dim_division_rebate_rate --不定时手工维护
    where dt ='$dwd_dim_division_rebate_rate_maxdt'
    ---in (select max(dt) from dwd_dim_division_rebate_rate)
;
drop table if exists tmp_dwd_fact_sales_order_invoice_default_rebate;
create table tmp_dwd_fact_sales_order_invoice_default_rebate stored as orc as
    select division, default_rate,cust_business_type
    from tmp_dwd_fact_sales_order_invoice_rebate rebate
    group by division, default_rate,cust_business_type
;
drop table if exists tmp_dwd_fact_sales_order_invoice_cust_rebate;
create table tmp_dwd_fact_sales_order_invoice_cust_rebate stored as orc as
    select dealer_code
         , bu
         , dealer_type
         , rebate_rate
         , upn
         , calculation_type ---- by customer
      from dwd_dim_customer_rebaterate
     where upn is null
       and start_date <= '${sync_date}'
       and end_date >= '${sync_date}'
       and calculation_type = '1'
    union all 
    select dealer_code
         , bu
         , dealer_type
         , rebate_rate
         , upn
         , calculation_type ---- by customer & UPN
      from dwd_dim_customer_rebaterate
     where upn is null
       and start_date <= '${sync_date}'
       and end_date >= '${sync_date}'
       and calculation_type = '2';
	  
drop table if exists tmp_dwd_fact_sales_order_invoice_new_rebate;
create table tmp_dwd_fact_sales_order_invoice_new_rebate stored as orc as
  select  bill_id
       ,accounting_no
       ,SUBSTRING(bill_date,0,10) as bill_date
       ,bill_date as bill_date_wz
       ,chinese_BillDate
       ,bill_type
       ,sales_id
       ,purchase_order
       ,material
       ,batch
       , case when sku.division_id='57' and soo.sales_type ='DR' then 0 else bill_qty end as bill_qty
       ,net_amount
       ,currency
       ,sold_to_pt
       ,sales_line
       ,delivery
       ,ship_to
       ,tax_amount
       ,tax_rate
        ,cust.level3_code
        ,cust.customer_type
         ,sku.division_display_name as division
           ,sku.default_location
           ,sku.sub_division
		   ,sku.sub_division_bak
           ,sku.business_group
           ,sales_type
    from ${target_db_name}.ods_so_invoice soo--源表 TRANS_Invoice  --每次sqlserver都推给我最近7天的数据
    left join tmp_dwd_fact_sales_order_invoice_cust cust on soo.sold_to_pt=cust.cust_account
    left outer join tmp_dwd_fact_sales_order_invoice_sku sku on soo.material=sku.material_code
    where soo.dt='$sync_date' and SUBSTRING(soo.bill_date,1,1)='2'
	and soo.currency='CNY';

drop table if exists tmp_dwd_fact_sales_order_invoice_combination_rebate;
create table tmp_dwd_fact_sales_order_invoice_combination_rebate stored as orc as
select 
	aa.purchase_order,
	aa.sub_division_bak,
	rr.product_level,
	rr.combo_group,
	rr.rate
from (select 
			distinct
			division,
			purchase_order, 
			sub_division_bak ,
			bill_date as  dt 
		from tmp_dwd_fact_sales_order_invoice_new_rebate  --取出bu和匹配的字段拿来匹配
		where division is not null 
			  and sub_division_bak is not null 
			  and purchase_order is not null) aa
inner join dwd_combination_rebate rr  
    on aa.division=rr.bu
	and aa.sub_division_bak=rr.product_level
	and aa.dt>=rr.start_date
	and aa.dt<=rr.end_date;
	
drop table if exists tmp_dwd_fact_sales_order_invoice_combination_rebate_list;
create table tmp_dwd_fact_sales_order_invoice_combination_rebate_list stored as orc as
select  distinct purchase_order,combx,rate --最后炸开去重
	from (
	SELECT purchase_order,combo_group,concat_ws(',',combo) as combo,rate   --数组转换成字符串
	FROM (
	SELECT purchase_order,combo_group,rate,combo,
	case when concat_ws(',',combo_group) =concat_ws(',',combo)  then 1 else 0 end as rn  --把数组转换成字符串比较出相等的标记为1
	FROM (	
		SELECT 
			purchase_order,
			sort_array(split(replace(replace(combo_group,'\[\"',''),'\"\]',''),'\"\,\"'))as combo_group, --把字符串转换成数组再排序
			rate,
			sort_array(collect_set(sub_division_bak)) AS combo  --把字段转换成数组再排序
		FROM tmp_dwd_fact_sales_order_invoice_combination_rebate
		GROUP BY purchase_order,combo_group,rate)X)Y
		WHERE Y.rn=1)z
	lateral view explode(split(replace(replace(replace(combo,' ',''),',','/'),'，','/'),'/')) t as combx
;
	
-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_sales_order_invoice partition(dt)
---这个地方不能去重因为真的有重复的数据
select  
        soi.bill_id
       ,soi.accounting_no
       ,soi.bill_date
       ,soi.bill_type
       ,soi.sales_id
       ,soi.delivery --delivery_no
       ,soi.material
       ,soi.sales_line --sales_line_no
       ,soi.batch
       ,soi.bill_qty
       ,soi.net_amount
       ,soi.currency
       ,soi.ship_to
       ,soi.sold_to_pt --sold_to
       ,soi.tax_amount
       ,soi.tax_rate
       ,soi.purchase_order
        , coalesce(soi.chinese_BillDate,soi.bill_date_wz) as chinese_BillDate
        ,soi.level3_code
        ,soi.customer_type
        ,coalesce( combination_rebate_list.rate,
			case when upper(soi.purchase_order) like '%SP' and soi.sub_division_bak in ('LeVeen针电极','Soloist针电极') then 0
              when upper(soi.purchase_order) like '%SP' and soi.material in ('M00562301','M00562321','M00562391'
                                                                                                    ,'M00561311'
                                                                                                    ,'M00562451'
                                                                                                    ,'M00562341'
                                                                                                    ,'M00562371') then 0
          else coalesce(case when rebate.division='EP' AND soi.bill_date>='2023-10-01' THEN 0.031 ELSE rebate.rate END, default_rebate.default_rate, 0) + coalesce(cust_rebate.rebate_rate,0) + coalesce(cust_rebate2.rebate_rate,0) end ) as rebate_rate -- PION SP&针电极
          ,soi.sales_type
          ,soi.division
       ,bill_date as dt  -- DynamicPartition
from tmp_dwd_fact_sales_order_invoice_new_rebate soi
left outer join tmp_dwd_fact_sales_order_invoice_rebate rebate on soi.division = rebate.division and array_contains(rebate.cust_business_type,soi.level3_code) and rebate.sub_divison = soi.sub_division_bak
left outer join tmp_dwd_fact_sales_order_invoice_default_rebate default_rebate on upper(default_rebate.division) = upper(soi.division) and array_contains(default_rebate.cust_business_type,soi.level3_code)
left outer join (
           select  dealer_code
                 , bu
                 , rebate_rate
		    from tmp_dwd_fact_sales_order_invoice_cust_rebate
		   where calculation_type = '1') cust_rebate on soi.division = cust_rebate.bu and soi.sold_to_pt = cust_rebate.dealer_code
left outer join (
           select  dealer_code
                 , upn
                 , rebate_rate
		    from tmp_dwd_fact_sales_order_invoice_cust_rebate
		   where calculation_type = '2') cust_rebate2 on soi.material = cust_rebate2.upn and soi.sold_to_pt = cust_rebate2.dealer_code
left outer join (
           select purchase_order
                 ,combx
				 ,rate 
			 from tmp_dwd_fact_sales_order_invoice_combination_rebate_list 
            where upper(purchase_order) like '%BM' )combination_rebate_list on soi.purchase_order=combination_rebate_list.purchase_order and soi.sub_division_bak=combination_rebate_list.combx
; 
"
# 3. 执行SQL，并判断查询结果是否为空,或者出现非CNY的数据
# count=`$hive -e "select count(1)x ,sum(case when currency='CNY' then 0 else 1 end)y from ${target_db_name}.ods_so_invoice where dt='$sync_date'and DATE(bill_date)='$yesterday'" | tail -n1`
# count_a=`echo "$count" | awk -F " " '{print $1}'`
# if [ $count_a -eq 0 ]; then
#   echo "Error: Failed to import data, count_a is zero."
#   exit 1
# fi
# count_b=`echo "$count" | awk -F " " '{print $2}'`
# if [ $count_b -ne 0 ]; then
#   echo "Error: Failed to count_b 不等于0,说明出现了非CNY的单位"
#   exit 1
# fi
# 3. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
echo "End syncing so invoice data DWD layer on ${sync_date} .................."