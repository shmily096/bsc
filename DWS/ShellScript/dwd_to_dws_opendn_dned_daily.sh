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
drop table if exists tmp_dws_opendn_dned_daily_a;
create table tmp_dws_opendn_dned_daily_a stored as orc as
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

drop table if exists tmp_dws_opendn_dned_daily_detail;
create table tmp_dws_opendn_dned_daily_detail stored as orc as
select     material
           ,SUM(qty) AS qty
           ,so_no
           ,delivery_id 
		   ,plant
		   ,pick_location_id
    from dwd_fact_sales_order_dn_detail
    where  dt>=date_add('$q_s_date',-1)
           and dt<='$q_e_date'
    group by
            material
           ,so_no
           ,delivery_id
		   ,plant
		   ,pick_location_id;
---已发货的qty
drop table if exists tmp_dws_opendn_dned_daily_odd;
create table tmp_dws_opendn_dned_daily_odd stored as orc as
select a.so_no
       ,a.created_datetime
       ,a.delivery_id
       ,a.dn_date
       ,detail.material
       ,detail.qty
	   ,detail.plant
	   ,detail.pick_location_id
from tmp_dws_opendn_dned_daily_detail detail 
inner join tmp_dws_opendn_dned_daily_a a on detail.so_no = a.so_no and detail.delivery_id = a.delivery_id
;
--开票的dn单
drop table if exists tmp_dws_opendn_dned_daily_soin;
create table tmp_dws_opendn_dned_daily_soin stored as orc as
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
---已发货的dn单，但是未开票的
drop table if exists tmp_dws_opendn_dned_daily_w;
create table tmp_dws_opendn_dned_daily_w stored as orc as
 select    odd.so_no
          ,odd.created_datetime
          ,odd.material
          ,odd.qty
          ,odd.dn_date
          ,odd.delivery_id
		  ,odd.plant
		  ,odd.pick_location_id
    from tmp_dws_opendn_dned_daily_odd odd
    left join (
               select sales_id,delivery_no,material 
			     from tmp_dws_opendn_dned_daily_soin soin
			   group by sales_id ,delivery_no,material) so_inv on odd.so_no = so_inv.sales_id and  odd.delivery_id =so_inv.delivery_no and odd.material=so_inv.material  -- 排除billed so 
	where so_inv.sales_id is null and so_inv.delivery_no is null and so_inv.material is null
;
---拿到订单表的数据
drop table if exists tmp_dws_opendn_dned_daily_soi;
create table tmp_dws_opendn_dned_daily_soi stored as orc as
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

drop table if exists tmp_dws_opendn_dned_daily_dned;
create table tmp_dws_opendn_dned_daily_dned stored as orc as
select  w.so_no
       ,w.material
       ,sum(w.qty*(soi.net_value/soi.qty))           as net_dned
       ,w.qty
       ,soi.division_id                              as division
       ,soi.pick_up_plant
	   ,soi.reference_po_number
       ,soi.if_cr
       ,w.dn_date
	   ,w.plant
	   ,w.pick_location_id
   from tmp_dws_opendn_dned_daily_w w
 left join tmp_dws_opendn_dned_daily_soi soi on w.so_no=soi.so_no and w.material=soi.material
where soi.if_kb = 0
group by  w.so_no
         ,w.material
         ,w.qty
         ,soi.division_id
         ,soi.pick_up_plant
		 ,soi.reference_po_number
         ,soi.if_cr
         ,w.dn_date
		 ,w.plant
		 ,w.pick_location_id
;
	
insert overwrite table ${target_db_name}.dws_opendn_dned_daily partition(dt_year, dt_month)
select  dned.so_no
       ,dned.material
       ,dned.qty
       ,dned.net_dned
       ,dned.division
       ,dned.pick_up_plant as plant_so
       ,dned.dn_date as dn_create_datetime
       ,dned.if_cr
	   ,dned.plant as plant_dn
	   ,dned.pick_location_id
       ,date_format(dned.dn_date,'yyyy') as dt_year 
       ,date_format(dned.dn_date,'MM')   as dt_month
from tmp_dws_opendn_dned_daily_dned dned
where dned.dn_date is not null
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
	   ,null as plant 
       ,null as pick_location_id
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_s_mon'  as dt_month
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
	   ,null as plant 
       ,null as pick_location_id
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_l_mon'  as dt_month
union all 
select  null    as so_no 
       ,null    as material 
       ,null    as qty
       ,0    as net_dned 
       ,null as division 
       ,null as pick_up_plant 
       ,null as dn_create_datetime
       ,0    as if_cr
	   ,null as plant 
       ,null as pick_location_id
       ,'${sync_date:0:4}'   as dt_year 
       ,'$q_e_mon'  as dt_month
;

"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  ${sync_date:0:4}: $q_s_mon:$q_l_mon:$q_e_mon  .................."