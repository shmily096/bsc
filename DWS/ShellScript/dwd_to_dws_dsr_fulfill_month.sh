#!/bin/bash
# Function:
#   sync up dws_dsr_fulfill_daily
# History:
# 2021-07-21    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
#有给定日期就跑给定日期对应的季度，没有就跑昨天对应的季度
if [ -n "$1" ] ;then 
    sync_date=$1
else
    #默认取昨天的日期
    sync_date=$(date -d '-1 day' +%F)
fi
this_year=${sync_date:0:4}-01-01
this_month=${sync_date:0:7}-01

dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`

echo "start syncing data into dws layer on on $q_s_date :$q_e_date:${this_month}:${this_year}
:$q_s_mon:$sync_date .................."
# open_order:not pgi

sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.parallel=false;


drop table if exists tmp_op_0;
create table tmp_op_0 stored as orc as
	       select  cast(cast(document as bigint) as string) as so_no
                  ,material
                  ,batch
                  ,sum(qty)            as qty
				  ,docdate             as created_datetime
                  ,sum(netvalue)       as net_value
                  ,lower(custponumber) as custponumber
                  ,division
                  ,rebate_rate
                  ,plant
                  ,request_delivery_date
                  ,case when (lower(custponumber) like '%cr' or lower(custponumber) like '%pro') then 1 else 0 end as is_cr
				  ,case when batch is not null and trim(batch) <> '' then 1 else 0 end as is_appointed
                  ,date_format('$sync_date', 'MM') as dt_mon
              from opsdw.dwd_fact_openordercn
             where insertdate>= '$sync_date'
			   and date_format(nvl(request_delivery_date,'$sync_date'),'yyyy-MM') = date_format('$sync_date','yyyy-MM')
               and dctp <>'KB'
			   and netvalue > 0
			   and (case when division = 'LTS' then 1 when division = 'LT' and dctp = 'DR' then 1 else 0 end <> 1)  -- 排除LTS和LT中DR订单
		  group by cast(cast(document as bigint) as string)
                  ,material
                  ,batch
				  ,docdate
                  ,lower(custponumber)
                  ,division
                  ,rebate_rate
                  ,plant
                  ,request_delivery_date
                  ,case when (lower(custponumber) like '%cr' or lower(custponumber) like '%pro') then 1 else 0 end
				  ,case when batch is not null and trim(batch) <> '' then 1 else 0 end;



drop table if exists tmp_op;
create table tmp_op stored as orc as
    select sales.*
		 , case when sales.custponumber like '%bm%' and m.material_flag = 1 then 1 end as delete_flag
	  from tmp_op_0 sales
	left outer join (
	             select material_code
				      , case when division_display_name = 'EP' and product_line1_code = '3201002' and upper(sap_upl_level4_name) like '%LSPRO%' then 1
                             when division_display_name = 'EP' and product_line1_code = '3201002' and upper(sap_upl_level4_name) like '%RHY%'   then 1
                             when division_display_name = 'EP' and product_line1_code = '3201002' and upper(sap_upl_level4_name) like '%MICROPACE%' then 1 
						end as material_flag
                   from dwd_dim_material 
                  where dt='$dwd_dim_material_maxdt'
                      ---in (select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
                       ) m on sales.material = m.material_code;

drop table if exists tmp_open_order;
create table tmp_open_order stored as orc as
    select *
	  from tmp_op
     where delete_flag is null;

drop table if exists tmp_deduct_qty;
create table tmp_deduct_qty stored as orc as
select material
     , plant
	 , sum(qty) as qty
     , dt_month
 from (
  --unpgied
    select sum(cast(qty as int)) as qty
         , material
         , plant
         , dt_month
    from dws_dsr_dned_daily
    where dt_year=date_format('$sync_date','YYYY')
      and cast(dt_month as int) =month('$sync_date')
    group by material
           , plant
           , dt_month
	union all 
   -- appointed order 
     select sum(qty) as qty
          , material
          , plant
		  , dt_mon as dt_month
	  from tmp_open_order 
	 where is_appointed = 1
	group by  material
           , plant
           , dt_mon  ) tot 
group by material
       , plant
       , dt_month;

drop table if exists tmp_onhand;
create table tmp_onhand stored as orc as
    select plant
           ,material
           ,sum(case when plant = 'D835' then unrestricted else unrestricted+inspection end ) as onhands_qty
           ,date_format(dt, 'MM') as dt_mon
    from dwd_fact_inventory_onhand
    where dt ='$sync_date'
      and inventory_type = 'DC'
      and plant in ('D835','D836','D837','D838')
      and unrestricted+inspection >0
    group by plant
            ,material
            ,date_format(dt, 'MM');

drop table if exists tmp_avai;
create table tmp_avai stored as orc as
    ----- 扣掉unpgi
     select aa.*
    from (
    ----- 扣掉unpgi + 指定的库存的openorder
    select (onhand.onhands_qty-nvl(d_qty.qty,0)) as available_qty
          , onhand.material
          , onhand.plant
          , onhand.dt_mon
      from tmp_onhand onhand
  left join tmp_deduct_qty d_qty on onhand.material = d_qty.material and onhand.plant = d_qty.plant ) aa 
     where aa.available_qty > 0;


insert overwrite table ${target_db_name}.dws_dsr_fulfill_daily partition(dt_year, dt_month)
select  open_order.qty                                   as open_qty 
       ,avai.available_qty                               as total_onhand_qty 
       ,open_order.created_datetime                      as order_datetime
       ,open_order.material 
       ,open_order.net_value 
       ,open_order.is_cr 
       ,open_order.division                           as division 
       ,open_order.rebate_rate
       ,open_order.plant
       ,sum(open_order.qty) over (partition by open_order.material order by open_order.created_datetime,open_order.is_cr rows between unbounded preceding and current row) as total_open_qty 
       ,sum(open_order.net_value) over (partition by open_order.material order by open_order.created_datetime,open_order.is_cr rows between unbounded preceding and current row) as total_value 
	   ,open_order.so_no
       ,date_format('$sync_date','YYYY')  as dt_year 
       ,date_format('$sync_date','MM')   as dt_month
 from (
      select *
	    from tmp_open_order
	  where is_appointed = 0 ) open_order
inner join tmp_avai avai on open_order.material = avai.material and open_order.plant = avai.plant and open_order.dt_mon=avai.dt_mon
union all 
       select qty                                 as open_qty 
             ,qty+0.2                             as total_onhand_qty 
             ,created_datetime                    as order_datetime
             ,material 
             ,net_value 
             ,is_cr 
             ,division
             ,rebate_rate
             ,plant
             ,qty       as total_open_qty 
             ,net_value as total_value 
	         ,so_no
             ,date_format('$sync_date','YYYY')  as dt_year 
             ,date_format('$sync_date','MM')   as dt_month
	    from tmp_open_order
	  where is_appointed = 1 
union all 
select  0    as open_qty 
       ,0    as total_onhand_qty 
       ,null as order_datetime
       ,null as material 
       ,null as net_value 
       ,null as is_cr 
       ,null as division 
       ,null as rebate_rate
       ,null as plant
       ,0    as total_open_qty 
       ,0    as total_value 
	   ,null as so_no
       ,date_format('$sync_date','YYYY')   as dt_year 
       ,date_format('$sync_date','MM')  as dt_month
;

"
delete_tmp="
drop table tmp_op;
drop table tmp_open_order;
drop table tmp_deduct_qty;
drop table tmp_onhand;
drop table tmp_avai;
"
dws_dsr_fulfill_monthly_sql="
use ${target_db_name};
set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
set hive.exec.parallel=false;

insert overwrite table dws_dsr_fulfill_monthly partition(dt_year, dt_month)
select              so_no
                    ,order_datetime
                    ,material
                    ,division
                    ,open_qty
                    ,qty
                    ,net_value
                    ,rebate_rate
                    ,is_cr
                    ,balance_tag
                    ,qty/open_qty*net_value as fulfill_amount
                    ,qty/open_qty*net_value*rebate_rate as fulfill_rebate
					,row_number() over (partition by dsr_year, dsr_year, material, balance_tag order by balance desc) rnk
                    , dsr_year
                    ,dsr_month                   
                from (
              select so_no
					,material
                    ,division
                    ,open_qty
                    ,order_datetime
                    ,is_cr
                    ,total_open_qty
                    ,total_onhand_qty
                    ,total_onhand_qty - total_open_qty     as balance
                    ,case when total_onhand_qty - total_open_qty > 0 then 0 else 1 end as balance_tag
                    ,case when total_onhand_qty - total_open_qty > 0 then open_qty else open_qty + total_onhand_qty - total_open_qty end as qty
                    ,net_value
                    ,rebate_rate
                    ,month('$sync_date')                 as dsr_month
                    ,year('$sync_date')                  as dsr_year
               from dws_dsr_fulfill_daily
              where dt_year='${sync_date:0:4}'
                and dt_month = cast(month('$sync_date') as int) 
                and so_no is not null
                and total_onhand_qty > 0 ) ful_raw
     union all 
             select  null as so_no
                    ,null as order_datetime
                    ,null as material
                    ,null as division
                    ,null as open_qty
                    ,null as qty
                    ,null as net_value
                    ,null as rebate_rate
                    ,null as is_cr
                    ,null as balance_tag
                    ,null as fulfill_amount
                    ,null as fulfill_rebate
					,null as  rnk
                    ,year('$sync_date')   as dsr_year
                    ,month('$sync_date')  as dsr_month                   
                ;
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"
#$hive -e "$delete_tmp"
$hive -e "$dws_dsr_fulfill_monthly_sql"

echo "End syncing data into DWS layer on  ${sync_date[year]} :${sync_date[month]}  .................."