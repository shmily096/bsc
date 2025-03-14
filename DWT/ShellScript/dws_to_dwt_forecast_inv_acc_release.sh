#!/bin/bash
# Function:
#   sync up dwt_dutycost_inoutbound_mr
# History:
# 2021-11-26  Amadna   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)
year_month=$(date  +'%Y-%m')

echo "start syncing dwt_dutycost_inoutbound_mr data into DWT layer on ${sync_date[month]} : ${sync_date[year]}"

# billed:
# dned:

# 1 Hive SQL string
dwt_sql="
use ${target_db_name};

set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
set hive.exec.parallel=false;


with a as(select 
    division_name
   ,sum(fore_invent_accrual) as fore_invent_accrual
   ,sum(fore_invent_accrual_usd) as fore_invent_accrual_usd
   ,dt_year 
   ,dt_month
from dws_duty_forecast__invent_accrual
where dt in (select max(dt) from dws_duty_forecast__invent_accrual)
      and division_name is not null
group by division_name, dt_year, dt_month)


--, b as (
select 
      division_name
      ,fore_invent_accrual
      ,fore_invent_accrual_usd
      ,dt_year 
      ,dt_month
      ,case when dt_month < 4 then 1
            when dt_month < 7 then 2
            when dt_month < 10 then 3
            else 4
       end as dt_quarter
from a
where dt_month in ('3','6','9','12')
)


insert overwrite table dwt_forecast_invent_accrual partition(dt_year, dt_month, dt_quarter,dt)
select 
      division_name
      ,fore_invent_accrual
      ,fore_invent_accrual_usd
      ,dt_year 
      ,dt_month
      ,case when cast(dt_month as int) < 4 then 1
            when cast(dt_month as int) < 7 then 2
            when cast(dt_month as int) < 10 then 3
            else 4
       end as dt_quarter
      ,'2021-12-28' as dt
from a


--invent_accrual = total accrual duty - last quarter total accrual duty
with c as (select division_name
      ,fore_invent_accrual
      ,fore_invent_accrual_usd
      ,lag(fore_invent_accrual,1,0) over (PARTITION by --cast(dt_quarter as int)
                                                            division_name
                                                            --,dt_year
                                                            order by cast(dt_quarter as int)
                                                            ,division_name
                                                            ,dt_year
                                                            ) as invent_accrual_realease
      ,lag(fore_invent_accrual_usd,1,0) over (PARTITION by --cast(dt_quarter as int)
                                                            division_name
                                                            --,dt_year
                                                            order by cast(dt_quarter as int)                                                             
                                                            ) as invent_accrual_realease_usd                                 
      ,dt_year
      ,dt_month
      ,dt_quarter
from ods_invent_accrual_history)


insert overwrite table dwt_forecast_inv_acc_release partition(dt_year, dt_month, dt_quarter,dt)
select division_name
      ,fore_invent_accrual
      ,fore_invent_accrual_usd
      ,fore_invent_accrual - invent_accrual_realease
      ,fore_invent_accrual_usd - invent_accrual_realease_usd                                 
      ,dt_year
      ,dt_month
      ,dt_quarters
      ,'2021-12-29' as dt
from c
where dt_quarter ='4'

;
"
# 2. 执行加载数据SQL
$hive -e "$dwt_sql"

echo "End syncing dwt_dsr_topic data into DWT layer on ${sync_date[month]} : ${sync_date[year]}"
