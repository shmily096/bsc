#!/bin/bash
# Function:
#   sync up dws_sale_order_leadtime_daily_trans 
# History:
# 2021-06-29    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
fi


declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."


sql_str="
use ${target_db_name};
-- 维度看销售时间，订单处理，发货处理，货物运输时间
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;



with a as (
    select
        sod.receiving_confirmation_date,
        sod.actual_gi_date,
        sod.created_datetime as so_dn_datetime,
        sod.so_no,
        sod.delivery_id,
        sodd.material,
        sodd.batch
    from (
        select receiving_confirmation_date
              ,actual_gi_date
              ,created_datetime
              ,so_no
              ,delivery_id
        from dwd_fact_sales_order_dn_info
        where actual_gi_date>=date_add('${sync_date[day]}',-7)
        group by
           receiving_confirmation_date
              ,actual_gi_date
              ,created_datetime
              ,so_no
              ,delivery_id
    ) sod
    inner join (
        select so_no
              ,delivery_id
              ,material
              ,batch
        from dwd_fact_sales_order_dn_detail
        where dt>=date_add('${sync_date[day]}',-7)
        and batch!=''
        group by
            so_no
              ,delivery_id
              ,material
              ,batch
        )sodd 
        on sod.so_no = sodd.so_no
        and sod.delivery_id = sodd.delivery_id
) ,
b as(
    select
        a.receiving_confirmation_date,
        a.actual_gi_date,
        a.so_dn_datetime,
        so.item_type,
        a.so_no,
        a.delivery_id,
        a.material,
        a.batch,
        so.created_datetime as so_create_datetime
    from
        a
    inner join (
        select item_type
              ,created_datetime
              ,so_no
              ,material
              ,batch
        from dwd_fact_sales_order_info 
        where dt>=date_add('${sync_date[day]}',-100)
        group by
            item_type
              ,created_datetime
              ,so_no
              ,material
              ,batch
        )so 
        on a.so_no = so.so_no
        and a.material = so.material
        ) ,
c as(
    select
        b.receiving_confirmation_date,
        b.actual_gi_date,
        b.so_dn_datetime,
        b.item_type,
        b.so_no,
        b.delivery_id,
        b.material,
        b.batch,
        b.so_create_datetime,
        round((unix_timestamp(b.so_dn_datetime) - unix_timestamp(b.so_create_datetime))/(60 * 60 * 24),
        1) as order_processing,
        round((unix_timestamp(b.actual_gi_date) - unix_timestamp(b.so_dn_datetime))/(60 * 60 * 24),
        1) as pgi_processing,
        round((unix_timestamp(b.receiving_confirmation_date) - unix_timestamp(b.actual_gi_date))/(60 * 60 * 24),
        1) as transport
    from b)

insert overwrite table ${target_db_name}.dws_sale_order_leadtime_daily_trans partition(dt)
select
    c.delivery_id,
    c.material,
    c.batch,
    c.item_type,
    c.so_create_datetime,
    c.so_dn_datetime,
    c.actual_gi_date,
    c.receiving_confirmation_date,
    c.order_processing,
    c.pgi_processing,
    c.transport,
    (c.order_processing + c.pgi_processing + c.transport) as product_sale,
    date_format(c.so_create_datetime,'yyyy-MM-dd') as dt
from c
group by
    c.delivery_id,
    c.material,
    c.batch,
    c.item_type,
    c.so_create_datetime,
    c.so_dn_datetime,
    c.actual_gi_date,
    c.receiving_confirmation_date,
    c.order_processing,
    c.pgi_processing,
    c.transport,
    date_format(c.so_create_datetime,'yyyy-MM-dd')
; 
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : $sync_date .................."