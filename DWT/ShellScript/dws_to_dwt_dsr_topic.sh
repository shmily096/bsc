#!/bin/bash
# Function:
#   sync up dwt_dsr_topic data to dwt layer
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
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
dwd_dim_dsr_le_srr_test_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_dsr_le_srr_test | awk '{print $8}' | grep -oP 20'[^ ]*' |awk 'BEGIN {max = 0} {if ($1 > max) max=$1} END {print max}'`
dwd_dim_exchange_rate_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_exchange_rate | awk '{print $8}' | grep -oP 20'[^ ]*' |awk 'BEGIN {max = 0} {if ($1 > max) max=$1} END {print max}'`
echo "start syncing dwt_dsr_topic data into DWT layer onon $q_s_date :$q_e_date:${this_month}:${this_year}:$q_s_mon:$sync_date :$mm.................."

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
with billed as 
(
        select  sum(if(net_billed is null,0,net_billed))       as net_billed
			  ,sum(if(bill_qty is null,0,bill_qty)) as bill_qty
              ,sum(if(billed_rebate is null,0,billed_rebate)) as billed_rebate
              ,division
              ,month(bill_date)                               as dsr_month
              ,year(bill_date)                                as dsr_year
        from dws_dsr_billed_daily
        where dt_year='${sync_date:0:4}'
            and dt_month between '$q_s_mon' and '$q_e_mon'
            and coalesce(upn_del_flag,0) =0
            and coalesce(cust_del_flag,0) =0
            and coalesce(OrderReason_del_flag,0) =0
            and coalesce(BillType_del_flag,0) =0
        group by  month(bill_date)
                ,year(bill_date)
                ,division
        union all
        select 
        -balance as net_billed
        ,0 AS bill_qty
        ,0 AS billed_rebate
        ,'LTS' AS division
        ,INT(period ) AS dsr_month
        ,year AS dsr_year
        from dwd_fact_trans_fs10n
        where cast(year as string)='${sync_date:0:4}'
        and int(period) between int('$q_s_mon') and int('$q_e_mon')
),
dned as 
(
        select  sum(if(net_dned is null,0,net_dned))   as net_dned
				,sum(qty) as pick_up_qty
              ,sum(if(dn_rebate is null,0,dn_rebate)) as dn_rebate
              ,division_display_name                  as division
              ,month(dn_create_datetime)              as dsr_month
              ,year(dn_create_datetime)               as dsr_year
        from dws_dsr_dned_daily
        where if_cr = '0' 
            and dt_year='${sync_date:0:4}'
            and dt_month between '$q_s_mon' and '$q_e_mon'
            and coalesce(upn_del_flag,0) =0
            and coalesce(cust_del_flag,0) =0
            and coalesce(OrderReason_del_flag,0) =0
            and coalesce(BillType_del_flag,0) =0
            and dn_create_datetime is not null
        group by  division_display_name
                ,month(dn_create_datetime)
                ,year(dn_create_datetime) 
),
cr_dned as 
(
        select  sum(if(net_dned is null,0,net_dned)) as net_cr_dned
				,sum(qty) as pick_up_qty_cr
                ,division_display_name                as division
                ,month(dn_create_datetime)            as dsr_month
                ,year(dn_create_datetime)             as dsr_year
        from dws_dsr_dned_daily
        where if_cr = '1' 
            and net_dned > 0
            and dt_year='${sync_date:0:4}'
            and dt_month between '$q_s_mon' and '$q_e_mon'
            and coalesce(upn_del_flag,0) =0
            and coalesce(cust_del_flag,0) =0
            and coalesce(OrderReason_del_flag,0) =0
            and coalesce(BillType_del_flag,0) =0
            and dn_create_datetime is not null
        group by  
                division_display_name
                ,month(dn_create_datetime)
                ,year(dn_create_datetime) 
),
credit as 
(
        select  sum(if(net_cr is null,0,net_cr)) as net_cr
                ,division_display_name            as division
                ,month(bill_date)                 as dsr_month
                ,year(bill_date)                  as dsr_year
				,sum(if(cr_qty is null,0,cr_qty)) as cr_qty
        from dws_dsr_cr_daily
        where dt_year='${sync_date:0:4}'
            and dt_month between '$q_s_mon' and '$q_e_mon'
            and coalesce(upn_del_flag,0) =0
            and coalesce(cust_del_flag,0) =0
            and coalesce(OrderReason_del_flag,0) =0
            and coalesce(BillType_del_flag,0) =0
        group by  
                division_display_name
                ,month(bill_date)
                ,year(bill_date)
),
ful as 
(
        select  sum(case when is_cr <> 1 then fulfill_amount else 0 end)    as net_fulfill
               ,sum(case when is_cr <> 1 then fulfill_rebate else 0 end)    as fulfill_rebate
               ,sum(case when is_cr = 1  then fulfill_amount else 0 end)    as fulfill_cr
               ,sum(case when is_cr <> 1 then qty else 0 end)  as available_qty
               ,sum(case when is_cr = 1 then qty else 0 end)   as cr_qty
               ,dt_month as dsr_month
               ,dt_year as dsr_year
               ,division
        from dws_dsr_fulfill_monthly
        where dt_year='${sync_date:0:4}'
          and dt_month >= '$q_s_mon' and dt_month <= month('${sync_date}')
          and so_no is not null  
          and (balance_tag = 0 or (balance_tag = 1 and rnk=1))
        group by  dt_month
                 ,dt_year
                 ,division
          
),
ls as (
   select last_value(le_cny) over(partition by division,year,month order by srr_version)  as le_cny
        ,last_value(le_usd)  over(partition by division,year,month order by srr_version)  as le_usd
        ,last_value(srr_cny) over(partition by division,year,month order by srr_version) as srr_cny
        ,last_value(srr_usd) over(partition by division,year,month order by srr_version) as srr_usd
        ,division
        ,year                                                              as dsr_year
        ,month                                                              as dsr_month
    from dwd_dim_dsr_le_srr
    where dt='$dwd_dim_dsr_le_srr_test_maxdt'
    ---in ( select max(dt) from dwd_dim_le_srr   ) 
)
,ex_ as(
    select rate, from_currency, valid_from 
     from dwd_dim_exchange_rate
    where from_currency='USD' and to_currency ='CNY'
    and dt= (select max(dt) from dwd_dim_exchange_rate)
    --'$dwd_dim_exchange_rate_maxdt'
	)
,ex_rate as(
    select cast(rate as decimal(9,2)) as rate, from_currency
    from ex_ where valid_from = (select max(valid_from) from ex_) )
,
uni as( 
    select  dsr_month
        ,dsr_year
        ,division
        ,sum(net_billed)     as net_billed
		,sum(bill_qty)     	 as bill_qty
        ,sum(billed_rebate)  as billed_rebate
        ,sum(net_dned)       as net_dned
        ,sum(dn_rebate)      as dn_rebate
        ,sum(net_cr_dned)    as net_cr_dned
        ,sum(net_cr)         as net_cr
        ,sum(net_fulfill)    as net_fulfill
        ,sum(fulfill_rebate) as fulfill_rebate
		,sum(available_qty)	 as available_qty
		,sum(pick_up_qty)	 as pick_up_qty
		,sum(pick_up_qty_cr) as pick_up_qty_cr
        ,'USD'               as currency
    from 
    (
        select  billed.dsr_month     as dsr_month
                ,billed.dsr_year      as dsr_year
                ,billed.division      as division
                ,billed.net_billed    as net_billed
				,billed.bill_qty 	  as bill_qty
                ,billed.billed_rebate as billed_rebate
                ,0                    as net_dned
                ,0                    as dn_rebate
                ,0                    as net_cr_dned
                ,0                    as net_cr
                ,0                    as net_fulfill
                ,0                    as fulfill_rebate
				,0				  	  as available_qty
				,0				  	  as pick_up_qty
				,0				  	  as pick_up_qty_cr
        from billed 
        union all
        select  dned.dsr_month as dsr_month
                ,dned.dsr_year  as dsr_year
                ,dned.division  as division
                ,0              as net_billed
				,0				as bill_qty
                ,0              as billed_rebate
                ,dned.net_dned  as net_dned
                ,dned.dn_rebate as dn_rebate
                ,0              as net_cr_dned
                ,0              as net_cr
                ,0              as net_fulfill
                ,0              as fulfill_rebate
				,0				as available_qty
				,dned.pick_up_qty
				,0				as pick_up_qty_cr
        from dned 
        union all
        select  cr_dned.dsr_month   as dsr_month
                ,cr_dned.dsr_year    as dsr_year
                ,cr_dned.division    as division
                ,0                   as net_billed
				,0					 as bill_qty
                ,0                   as billed_rebate
                ,0                   as net_dned
                ,0                   as dn_rebate
                ,cr_dned.net_cr_dned as net_cr_dned
                ,0                   as net_cr
                ,0                   as net_fulfill
                ,0                   as fulfill_rebate
				,0				     as available_qty
				,0					 as pick_up_qty
				,cr_dned.pick_up_qty_cr
        from cr_dned 
        union all
        select  credit.dsr_month as dsr_month
                ,credit.dsr_year  as dsr_year
                ,credit.division  as division
                ,0                as net_billed
				,0	              as bill_qty
                ,0                as billed_rebate
                ,0                as net_dned
                ,0                as dn_rebate
                ,0                as net_cr_dned
                ,credit.net_cr    as net_cr
                ,0                as net_fulfill
                ,0                as fulfill_rebate
				,0				  as available_qty
				,0				  as pick_up_qty
				,credit.cr_qty    as pick_up_qty_cr  ---total cr
        from credit 
        union all
         select  ful.dsr_month      as dsr_month
                ,ful.dsr_year       as dsr_year
                ,ful.division       as division
                ,0                  as net_billed
				,0					as bill_qty
                ,0                  as billed_rebate
                ,0                  as net_dned
                ,0                  as dn_rebate
                ,0                  as net_cr_dned
                ,0                  as net_cr
                ,(nvl(ful.net_fulfill,0) + nvl(fulfill_cr,0) - nvl(ful.fulfill_rebate,0))  as net_fulfill   --- available 2 fulfill 单独归到自己这里
                ,0                  as fulfill_rebate
				,ful.available_qty + ful.cr_qty  as available_qty
				,0				    as pick_up_qty
				,0			        as pick_up_qty_cr
        from ful
        union all 
		select cast('$q_s_mon' as int)  as dsr_month
             , cast('${sync_date:0:4}' as int)   as dsr_year
			 , division
			 , coalesce(net_billed,0) as net_billed
			 , 0 as bill_qty
			 , coalesce(billed_rebate,0) as billed_rebate
			 , coalesce(net_dned,0) as net_dned
			 , coalesce(dn_rebate,0) as dn_rebate
			 , coalesce(net_cr_dned,0) as net_cr_dned
			 , coalesce(net_cr,0) as net_cr
			 , coalesce(net_fulfill,0) as net_fulfill
			 , coalesce(fulfill_rebate,0) as fulfill_rebate
			 ,0				  as available_qty
			 ,0				  as pick_up_qty
			 ,0				  as pick_up_qty_cr
		 from dwd_dsr_topic_nonvalue  --本来就是空的
         union all 
		select cast('$q_l_mon' as int)  as dsr_month
             , cast('${sync_date:0:4}' as int)   as dsr_year
			 , division
			 , coalesce(net_billed,0) as net_billed
			 , 0 as bill_qty
			 , coalesce(billed_rebate,0) as billed_rebate
			 , coalesce(net_dned,0) as net_dned
			 , coalesce(dn_rebate,0) as dn_rebate
			 , coalesce(net_cr_dned,0) as net_cr_dned
			 , coalesce(net_cr,0) as net_cr
			 , coalesce(net_fulfill,0) as net_fulfill
			 , coalesce(fulfill_rebate,0) as fulfill_rebate
			 ,0				  as available_qty
			 ,0				  as pick_up_qty
			 ,0				  as pick_up_qty_cr
		 from dwd_dsr_topic_nonvalue  --本来就是空的
         union all 
		select cast('$q_e_mon' as int)  as dsr_month
             , cast('${sync_date:0:4}' as int)   as dsr_year
			 , division
			 , coalesce(net_billed,0) as net_billed
			 , 0 as bill_qty
			 , coalesce(billed_rebate,0) as billed_rebate
			 , coalesce(net_dned,0) as net_dned
			 , coalesce(dn_rebate,0) as dn_rebate
			 , coalesce(net_cr_dned,0) as net_cr_dned
			 , coalesce(net_cr,0) as net_cr
			 , coalesce(net_fulfill,0) as net_fulfill
			 , coalesce(fulfill_rebate,0) as fulfill_rebate
			 ,0				  as available_qty
			 ,0				  as pick_up_qty
			 ,0				  as pick_up_qty_cr
		 from dwd_dsr_topic_nonvalue  --本来就是空的
    ) aaa
    group by  dsr_month
            ,dsr_year
            ,division
),
 wd as (
select 
    year as cal_year
    , cast(month as int) as cal_month
    , count(case when workdayflag='Y' then yearmonthdate end) as total_workday
    , count(case when workdayflag='Y' and yearmonthdate<='$sync_date' then yearmonthdate end) as total_workdaybytoday
from dwd_dim_calendar_dw 
where year = '${sync_date:0:4}'
and month between  cast(cast('$q_s_mon' as int) as string) and cast(cast('$q_e_mon' as int)as string)
group by year
, month

) 

insert overwrite table dwt_dsr_topic partition(dt_year, dt_month)
select  uni.dsr_month 			as bill_month
       ,uni.dsr_year 			as bill_year
       ,uni.division 			as division
       ,uni.net_billed 			as net_amount_shiped
       ,uni.net_dned 			as net_amount_dn
       ,uni.net_fulfill 		as fulfilled_value
       ,(uni.billed_rebate + uni.dn_rebate) as rebate_value  
       ,(uni.net_cr + uni.net_cr_dned)  as net_cr_shiped    
       ,(uni.net_billed + uni.net_dned +uni.net_fulfill - uni.billed_rebate - uni.dn_rebate  + uni.net_cr + uni.net_cr_dned)      as estimate_index
       ,(uni.net_billed + uni.net_fulfill)  as shiped_and_fulfill
       ,uni.net_billed/ex_rate.rate         as net_amount_shiped_usd
       ,uni.net_dned/ex_rate.rate           as net_amount_dn_usd
       ,uni.net_fulfill/ex_rate.rate        as fulfilled_value_usd
       ,(uni.billed_rebate + uni.dn_rebate )/ex_rate.rate   as rebate_value_usd
       ,(uni.net_cr + uni.net_cr_dned)/ex_rate.rate      as net_cr_shiped_usd
       ,(uni.net_billed + uni.net_dned +uni.net_fulfill - uni.billed_rebate - uni.dn_rebate + uni.net_cr + uni.net_cr_dned)/ex_rate.rate  as estimate_index_usd
       ,(uni.net_billed + uni.net_fulfill)/ex_rate.rate      as shiped_and_fulfill_usd
       ,ls.le_cny       		as le_cny
       ,ls.le_usd       		as le_usd
       ,ls.srr_cny      		as srr_cny
       ,ls.srr_usd      		as srr_usd 
	   ,uni.bill_qty 			as bill_qty
	   ,uni.available_qty		as available_qty
	   ,uni.pick_up_qty			as pick_up_qty
	   ,uni.pick_up_qty_cr		as pick_up_qty_cr
	   ,wd.total_workday
	   ,wd.total_workdaybytoday
       ,uni.dsr_year    		as dt_year 
       ,uni.dsr_month   		as dt_month
from uni
left join ls on uni.dsr_month=ls.dsr_month 
    and uni.dsr_year=ls.dsr_year 
    and uni.division=ls.division
left join ex_rate on uni.currency = ex_rate.from_currency
left join wd 	  on uni.dsr_year=wd.cal_year and uni.dsr_month=wd.cal_month
;
"

# 2. 执行加载数据SQL
echo "$dwt_sql"
$hive -e "$dwt_sql"
echo "End syncing data into DWT layer on  ${sync_date[year]} :${sync_date[month]}  .................."
