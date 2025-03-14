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
    #默认取昨天的日期
    sync_date=$(date -d '-1 day' +%F)
fi
this_year=${sync_date:0:4}-01-01
this_month=${sync_date:0:7}-01
mm=10#${sync_date:5:2}
if ((mm >= 1 ))&&((mm <= 3 ));then
    q_s_date=${sync_date:0:4}-01-01
    q_s_mon=01
    q_l_date=${sync_date:0:4}-02-01
    q_l_mon=02
    q_e_date=${sync_date:0:4}-03-31
    q_e_mon=03
elif ((mm >= 4 ))&&((mm <= 6 ));then
    q_s_date=${sync_date:0:4}-04-01
    q_s_mon=04
    q_l_date=${sync_date:0:4}-05-01
    q_l_mon=05
    q_e_date=${sync_date:0:4}-06-30
    q_e_mon=06
elif ((mm >= 7 ))&&((mm <= 9 ));then
    q_s_date=${sync_date:0:4}-07-01
    q_s_mon=07
    q_l_date=${sync_date:0:4}-08-01
    q_l_mon=08
    q_e_date=${sync_date:0:4}-09-30
    q_e_mon=09
elif ((mm >= 10 ))&&((mm <= 12 ));then
    q_s_date=${sync_date:0:4}-10-01
    q_s_mon=10
    q_l_date=${sync_date:0:4}-11-01
    q_l_mon=11
    q_e_date=${sync_date:0:4}-12-31
    q_e_mon=12
fi
dwd_dim_customer_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_customer | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $6}'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $6}'`
dwd_dim_division_rebate_rate_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_division_rebate_rate | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing data into dws layer on $q_s_date :$q_e_date:${this_month}:${this_year}:$q_s_mon:$sync_date :$mm.................."
#a--sodn中取 有dn create时间没有pgi的所有so单
#w--sodndeatil取qty
#soi--总qty
#soin--用于rebate筛选
#reb--rebate
#dned--通过dn detail得到qty和总qty比较 算正在发货的钱
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
drop table if exists tmp_dws_dsr_dned_daily_a;
create table tmp_dws_dsr_dned_daily_a stored as orc as
    select so_no 
           ,max(chinese_dncreatedt) as created_datetime 
           ,delivery_id
           ,max(to_date(chinese_dncreatedt)) as dn_date  --chinese_dncreatedt  替换
    from ${target_db_name}.dwd_fact_sales_order_dn_info
    where (actual_gi_date is null or actual_gi_date ='') 
            and dt>=date_add('$q_s_date',-1)
            and dt<='$q_e_date'
            and to_date(chinese_dncreatedt)>='$q_s_date'
            and to_date(chinese_dncreatedt)<='$q_e_date'
    group by
            so_no 
           ,delivery_id
;

drop table if exists tmp_dws_dsr_dned_daily_detail;
create table tmp_dws_dsr_dned_daily_detail stored as orc as
select     material
           ,SUM(qty) AS qty
           ,so_no
           ,delivery_id 
    from dwd_fact_sales_order_dn_detail
    where  dt>=date_add('$q_s_date',-1)
           and dt<='$q_e_date'
    group by
            material
           ,so_no
           ,delivery_id;

drop table if exists tmp_dws_dsr_dned_daily_odd;
create table tmp_dws_dsr_dned_daily_odd stored as orc as
select a.so_no
       ,a.created_datetime
       ,a.delivery_id
       ,a.dn_date
       ,detail.material
       ,detail.qty
from tmp_dws_dsr_dned_daily_detail detail 
inner join tmp_dws_dsr_dned_daily_a a on detail.so_no = a.so_no and detail.delivery_id = a.delivery_id
;
drop table if exists tmp_dws_dsr_dned_daily_soin;
create table tmp_dws_dsr_dned_daily_soin stored as orc as
    select material 
           ,sales_id 
           ,sold_to 
           ,purchase_order
           ,bill_type
           ,delivery_no
    from dwd_fact_sales_order_invoice
    where dt>=date_add('$q_s_date',-1)
      and dt<='$q_e_date'
;
drop table if exists tmp_dws_dsr_dned_daily_w;
create table tmp_dws_dsr_dned_daily_w stored as orc as
 select    odd.so_no
          ,odd.created_datetime
          ,odd.material
          ,odd.qty
          ,odd.dn_date
          ,odd.delivery_id
    from tmp_dws_dsr_dned_daily_odd odd
    left join (
               select sales_id,delivery_no,material 
			     from tmp_dws_dsr_dned_daily_soin soin
			   group by sales_id ,delivery_no,material) so_inv on odd.so_no = so_inv.sales_id and  odd.delivery_id =so_inv.delivery_no and odd.material=so_inv.material  -- 排除billed so 
	where so_inv.sales_id is null and so_inv.delivery_no is null and so_inv.material is null
;
drop table if exists tmp_dws_dsr_dned_daily_soi;
create table tmp_dws_dsr_dned_daily_soi stored as orc as
 select so_no
           ,material
           ,sum(qty) as qty
           ,sum(net_value) as net_value
           ,division_id
           ,rebate_rate
           ,pick_up_plant
		   ,lower(reference_po_number) as reference_po_number
           ,order_reason
           ,customer_code
           ,case when (lower(reference_po_number) like '%cr' or lower(reference_po_number) like '%pro') then 1 
                 when (lower(reference_po_number) not like '%cr' or lower(reference_po_number) not like '%pro') then 0 
             end as if_cr
		   , case when order_type = 'KB' then 1 else 0 end as if_kb
    from dwd_fact_sales_order_info
    where dt>=date_add('${this_year}',-1)
    group by
            so_no
           ,material
           ,division_id
           ,rebate_rate
           ,pick_up_plant
           ,lower(reference_po_number)
           ,order_reason
           ,customer_code
		   ,case when (lower(reference_po_number) like '%cr' or lower(reference_po_number) like '%pro') then 1 
                 when (lower(reference_po_number) not like '%cr' or lower(reference_po_number) not like '%pro') then 0 end
		   , case when order_type = 'KB' then 1 else 0 end
; 
drop table if exists tmp_dws_dsr_dned_daily_reb;
create table tmp_dws_dsr_dned_daily_reb stored as orc as
select soi.material 
           ,soi.so_no 
           ,soi.rebate_rate
           ,soi.order_reason
           ,soinv.bill_type
           ,soi.customer_code
		   ,soi.reference_po_number
    from tmp_dws_dsr_dned_daily_soi soi
    left join (
        select  material 
               ,sales_id 
               ,sold_to 
               ,purchase_order
               ,bill_type
        from tmp_dws_dsr_dned_daily_soin soin
        where sold_to in ('554263', '107352') 
           or (lower(material) in ('pd525', 'pd275', 'pd875') or substr(lower(material), 1, 3)='rbt')  
        group by
                material 
               ,sales_id 
               ,sold_to 
               ,purchase_order
               ,bill_type )soinv on soinv.material = soi.material and soinv.sales_id = soi.so_no
    where soinv.material is null and soinv.sales_id is null and lower(substring(soi.reference_po_number, 1, 5)) != 'price' 
    group by
           soi.material 
           ,soi.so_no 
           ,soi.rebate_rate
           ,soi.order_reason
           ,soinv.bill_type
           ,soi.customer_code
		   ,soi.reference_po_number
;
drop table if exists tmp_dws_dsr_dned_daily_dned;
create table tmp_dws_dsr_dned_daily_dned stored as orc as
select  w.so_no
       ,w.material
       ,sum(w.qty*(soi.net_value/soi.qty))           as net_dned
       ,w.qty
       ,soi.division_id                              as division
       ,soi.pick_up_plant
	   ,soi.reference_po_number
       ,soi.if_cr
       ,w.dn_date
   from tmp_dws_dsr_dned_daily_w w
 left join tmp_dws_dsr_dned_daily_soi soi on w.so_no=soi.so_no and w.material=soi.material
where soi.if_kb = 0
group by  w.so_no
         ,w.material
         ,w.qty
         ,soi.division_id
         ,soi.pick_up_plant
		 ,soi.reference_po_number
         ,soi.if_cr
         ,w.dn_date
;
drop table if exists tmp_dws_dsr_dned_daily_divi;
create table tmp_dws_dsr_dned_daily_divi stored as orc as
    select material_code
        ,division_display_name
        ,sub_division
		,sub_division_bak
        ,case when (lower(material_code)) like 'rbt%' then 1
            ----  when  (lower(product_line1_code)) = '3201001' and  division_display_name<>'EP'then 1
            ----  when division_display_name='EP' and  (lower(product_line1_code)) = '3201002' then 1
           else 0 end as upn_del_flag
		, product_line1_code
		, upper(sap_upl_level4_name) as sap_upl_level4_name
    from dwd_dim_material 
    where  dt ='$dwd_dim_material_maxdt'
    --in (select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
    group by material_code
        ,division_display_name
        ,sub_division
		,sub_division_bak
        ,product_line1_code
		,upper(sap_upl_level4_name)
;
drop table if exists tmp_dws_dsr_dned_daily_cust;
create table tmp_dws_dsr_dned_daily_cust stored as orc as
    select cust_account
          ,type_of_business
		  ,level3_code
          ,case when (lower(cust_account)) like 'pd%' or (upper(type_of_business)) ='INTER-SA-SRR' then 1 else 0 end as cust_del_flag
     from dwd_dim_customer
    where 
    --dt ='$dwd_dim_customer_maxdt'
    dt  in (select max(dt) from dwd_dim_customer where dt>=date_sub('$dwd_dim_customer_maxdt',7))
  group by cust_account
          ,type_of_business
		  ,level3_code
;
drop table if exists tmp_dws_dsr_dned_daily_new_reb;
create table tmp_dws_dsr_dned_daily_new_reb stored as orc as
select  reb.material 
        ,reb.so_no 
        ,reb.rebate_rate 
        ,reb.order_reason
        ,reb.bill_type
        ,reb.customer_code
		,reb.reference_po_number as po_number
        ,cust.cust_del_flag
		,cust.level3_code
		,sku.division_display_name as division
		,sku.sub_division_bak
    from tmp_dws_dsr_dned_daily_reb reb 
	left join tmp_dws_dsr_dned_daily_cust cust on reb.customer_code = cust.cust_account 
	left join tmp_dws_dsr_dned_daily_divi sku on reb.material=sku.material_code 	
  ;
  
drop table if exists tmp_dws_dsr_dned_daily_rebate;
create table tmp_dws_dsr_dned_daily_rebate stored as orc as
    select * from dwd_dim_division_rebate_rate --不定时手工维护
    where dt='$dwd_dim_division_rebate_rate_maxdt' 
    ---in (select max(dt) from dwd_dim_division_rebate_rate)
;

drop table if exists tmp_dws_dsr_dned_daily_default_rebate;
create table tmp_dws_dsr_dned_daily_default_rebate stored as orc as
    select division, default_rate,cust_business_type
    from tmp_dws_dsr_dned_daily_rebate
    group by division, default_rate,cust_business_type
;
					 
drop table if exists tmp_dws_dsr_dned_daily_default_cust_rebate;
create table tmp_dws_dsr_dned_daily_default_cust_rebate stored as orc as
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

drop table if exists tmp_dws_dsr_dned_daily_combination_rebate;
create table tmp_dws_dsr_dned_daily_combination_rebate stored as orc as
select 
	aa.po_number,
	aa.sub_division_bak,
	rr.product_level,
	rr.combo_group,
	rr.rate
from (select 
			distinct
			division,
			po_number, 
			sub_division_bak 
		from tmp_dws_dsr_dned_daily_new_reb  --取出bu和匹配的字段拿来匹配
		where division is not null 
			  and sub_division_bak is not null 
			  and po_number is not null) aa
inner join dwd_combination_rebate rr  
    on aa.division=rr.bu
	and aa.sub_division_bak=rr.product_level
	and rr.start_date<='${sync_date}'
	and rr.end_date>='${sync_date}';

drop table if exists tmp_dws_dsr_dned_daily_combination_rebate_list;
create table tmp_dws_dsr_dned_daily_combination_rebate_list stored as orc as
select  distinct po_number,combx,rate --最后炸开去重
	from (
	SELECT po_number,combo_group,concat_ws(',',combo) as combo,rate   --数组转换成字符串
	FROM (
	SELECT po_number,combo_group,rate,combo,
	case when concat_ws(',',combo_group) =concat_ws(',',combo)  then 1 else 0 end as rn  --把数组转换成字符串比较出相等的标记为1
	FROM (	
		SELECT 
			po_number,
			sort_array(split(replace(replace(combo_group,'\[\"',''),'\"\]',''),'\"\,\"'))as combo_group, --把字符串转换成数组再排序
			rate,
			sort_array(collect_set(sub_division_bak)) AS combo  --把字段转换成数组再排序
		FROM tmp_dws_dsr_dned_daily_combination_rebate
		GROUP BY po_number,combo_group,rate)X)Y
		WHERE Y.rn=1)z
	lateral view explode(split(replace(replace(replace(combo,' ',''),',','/'),'，','/'),'/')) t as combx
;

drop table if exists tmp_dws_dsr_dned_daily_last_reb;
create table tmp_dws_dsr_dned_daily_last_reb stored as orc as
select
		reb.material 
        ,reb.so_no 
        ,reb.order_reason
        ,reb.bill_type
        ,reb.customer_code
		,reb.po_number
        ,reb.cust_del_flag
		,reb.level3_code
		,reb.division
		,reb.sub_division_bak
		 ,coalesce( combination_rebate_list.rate,
			case when upper(reb.po_number) like '%SP' and reb.sub_division_bak in ('LeVeen针电极','Soloist针电极') then 0
              when upper(reb.po_number) like '%SP' and reb.material in ('M00562301','M00562321','M00562391'
                                                                                                    ,'M00561311'
                                                                                                    ,'M00562451'
                                                                                                    ,'M00562341'
                                                                                                    ,'M00562371') then 0
          else coalesce( case when rebate.division='EP'  THEN 0.031 ELSE rebate.rate END, default_rebate.default_rate, 0) + coalesce(cust_rebate.rebate_rate,0) end ) as rebate_rate
from tmp_dws_dsr_dned_daily_new_reb reb
left outer join tmp_dws_dsr_dned_daily_rebate rebate on reb.division = rebate.division 
    and array_contains(rebate.cust_business_type,reb.level3_code)
    and rebate.sub_divison = reb.sub_division_bak
left outer join tmp_dws_dsr_dned_daily_default_rebate default_rebate on default_rebate.division = reb.division 
		and  array_contains(default_rebate.cust_business_type,reb.level3_code) 
--left outer join tmp_dws_dsr_dned_daily_default_cust_rebate cust_rebate on reb.division = cust_rebate.bu 
--	and reb.customer_code = cust_rebate.dealer_code
left outer join (
           select  dealer_code
                 , bu
                 , rebate_rate
		    from tmp_dws_dsr_dned_daily_default_cust_rebate
		   where calculation_type = '1') cust_rebate on reb.division = cust_rebate.bu and reb.customer_code = cust_rebate.dealer_code
left outer join (
           select  dealer_code
                 , upn
                 , rebate_rate
		    from tmp_dws_dsr_dned_daily_default_cust_rebate
		   where calculation_type = '2') cust_rebate2 on reb.material = cust_rebate2.upn and reb.customer_code = cust_rebate2.dealer_code
left outer join (select po_number,combx,rate from tmp_dws_dsr_dned_daily_combination_rebate_list 
                    where upper(po_number) like '%BM' )combination_rebate_list 
	on reb.po_number=combination_rebate_list.po_number 
	and reb.sub_division_bak=combination_rebate_list.combx;

	
insert overwrite table ${target_db_name}.dws_dsr_dned_daily partition(dt_year, dt_month)
select  dned.so_no
       ,dned.material
       ,dned.qty
       ,dned.net_dned
       ,(dned.net_dned*new_reb.rebate_rate)             as dn_rebate
       ,dned.division
       ,dned.pick_up_plant
       ,dned.dn_date as dn_create_datetime
       ,dned.if_cr
       ,divi.upn_del_flag
       ,new_reb.cust_del_flag
       ,case when divi.division_display_name = 'URO' and new_reb.order_reason='W18' then 1
             when divi.division_display_name = 'EP'  and dned.reference_po_number like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%LSPRO%' then 1
             when divi.division_display_name = 'EP'  and dned.reference_po_number like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%RHY%' then 1
             when divi.division_display_name = 'EP'  and dned.reference_po_number like '%bm%' and divi.product_line1_code = '3201002' and divi.sap_upl_level4_name like '%MICROPACE%' then 1
         else 0
       end as orderreason_del_flag
       ,case when divi.division_display_name = 'ENDO' and new_reb.bill_type='ZIVR' then 1
       else 0
       end as billtype_del_flag
       ,date_format(dned.dn_date,'yyyy') as dt_year 
       ,date_format(dned.dn_date,'MM')   as dt_month
       --,dned.dn_date as dt
from tmp_dws_dsr_dned_daily_dned dned
left join tmp_dws_dsr_dned_daily_last_reb new_reb on dned.so_no = new_reb.so_no and dned.material = new_reb.material
left join tmp_dws_dsr_dned_daily_divi divi on dned.material = divi.material_code 
where dned.dn_date is not null
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,0    as dn_rebate 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
       ,0    as upn_del_flag 
       ,0    as cust_del_flag 
	   ,0    as orderreason_del_flag
       ,0    as billtype_del_flag
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_s_mon'  as dt_month
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,0    as dn_rebate 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
       ,0    as upn_del_flag 
       ,0    as cust_del_flag 
	   ,0    as orderreason_del_flag
       ,0    as billtype_del_flag
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_l_mon'  as dt_month
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,0    as dn_rebate 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
       ,0    as upn_del_flag 
       ,0    as cust_del_flag 
	   ,0    as orderreason_del_flag
       ,0    as billtype_del_flag
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_e_mon'  as dt_month
;

"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  ${sync_date:0:4}: $q_s_mon:$q_l_mon:$q_e_mon  .................."