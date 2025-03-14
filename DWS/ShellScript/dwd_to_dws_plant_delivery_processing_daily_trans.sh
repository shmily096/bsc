#!/bin/bash
# Function:
#   sync up dws_plant_delivery_processing_daily_trans 
# History:
# 2021-07-07    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# if [ -n "$1" ] ;then 
#     sync_date=$1
# else
#     sync_date=$(date  +%F)
# fi

if [ -n "$1" ] ;then 
    sync_year=$1
else
    sync_year=$(date  +'%Y')
fi

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."



sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

with a as (
    select  sod.created_datetime as so_dn_datetime
        ,sod.actual_gi_date
        ,sod.so_no
        ,sodd.material
        ,sodd.batch
    from (
        select actual_gi_date
              ,created_datetime
              ,so_no
              ,delivery_id
        from dwd_fact_sales_order_dn_info
        where actual_gi_date>=date_add('${sync_date[day]}',-7)
        group by
           actual_gi_date
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
    on sod.so_no = sodd.so_no and sod.delivery_id = sodd.delivery_id
    
)


insert overwrite table dws_plant_delivery_processing_daily_trans partition(dt)
select  b.pick_up_plant
       ,round((unix_timestamp(b.actual_gi_date) - unix_timestamp(b.so_dn_datetime))/(60*60*24),1) as pgi_processing
       ,b.actual_gi_date
       ,b.so_no
        ,b.material
        ,b.batch
        ,date_format(b.actual_gi_date,'yyyy-MM-dd')                                                  as dt
from 
(
    select  a.so_dn_datetime
        ,a.actual_gi_date
        ,a.so_no
        ,a.material
        ,a.batch
        ,so.pick_up_plant
    from a
    inner join (
        select pick_up_plant
              ,so_no
              ,material
              ,batch
        from dwd_fact_sales_order_info 
        where dt>=date_add('${sync_date[day]}',-100)
        and pick_up_plant is not null  
        group by
           pick_up_plant
              ,so_no
              ,material
              ,batch
        )so
    on a.material = so.material  and a.so_no = so.so_no
) b
group by
    b.pick_up_plant
    ,b.actual_gi_date
    ,date_format(b.actual_gi_date,'yyyy-MM-dd')
    ,b.so_no
    ,b.material
    ,b.batch
    ,b.so_dn_datetime

    
; 
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year :  .................."