#!/bin/bash
# Function:
#   sync up xxxx
# History:
# 2021-07-21    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)


echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."

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
WITH q1 AS
(
    SELECT  fin_year
           ,division
           ,case when dealer_type='LP' and division = 'IC' then 'IC'
               when dealer_type='LP' and division = 'PION' then 'PION'
               else sub_buname
               end as sub_buname
           ,dealer_name
           ,sapi_d
           ,parent_dealer_name
           ,dealer_type
           ,SUM(q1_amount) AS q1_amount
           ,SUM(q2_amount) AS q2_amount
           ,SUM(q3_amount) AS q3_amount
           ,SUM(q4_amount) AS q4_amount
    FROM dwd_fact_dealer_purchase_quotation
    WHERE fin_year ='${sync_date[year]}'
    AND dt IN ( SELECT MAX(dt) FROM dwd_fact_dealer_purchase_quotation)
    GROUP BY  fin_year
             ,division
             ,sub_buname
             ,dealer_name
             ,sapi_d
             ,parent_dealer_name
             ,dealer_type
),
a AS
(
    SELECT  division
           ,sub_division
           ,customer_code
           ,cust_level3
           ,bill_year
           ,quarter
           ,dealer_name
           ,parent_dealer_name
           ,SUM(if(dealer_complete is null, 0, dealer_complete)) as dealer_quar_complete
    FROM opsdw.dws_dsr_dealer_daily_transation
    WHERE dt_year='${sync_date[year]}' 
          and quarter in ( select quarter from dws_dsr_dealer_daily_transation 
              where bill_month = month('${sync_date[day]}')
              group by quarter)
    GROUP BY  division
             ,sub_division
             ,customer_code
             ,cust_level3
             ,bill_year
             ,quarter
             ,dealer_name
             ,parent_dealer_name
)
, aa as (
       select division,
           case when cust_level3='LP' and division = 'IC' then 'IC'
          when cust_level3='LP' and division = 'PION' then 'PION'
          end as sub_division
           ,customer_code
           ,cust_level3
           ,bill_year
           ,quarter
           ,dealer_name
           ,parent_dealer_name
           ,SUM(dealer_quar_complete) dealer_quar_complete
       from a 
       where cust_level3='LP' and division in('IC','PION')
       group by
           division
           ,customer_code
           ,cust_level3
           ,bill_year
           ,quarter
           ,dealer_name
           ,parent_dealer_name
)
INSERT OVERWRITE TABLE dwt_dsr_dealer_quarter_trans partition(dt_year, dt_quarter)
SELECT  q1.division
       ,q1.sub_buname
       ,q1.dealer_type
       ,q1.fin_year
       ,quarter('${sync_date[day]}') as quarter
       ,q1.dealer_name
       ,a.dealer_quar_complete
       ,case quarter('${sync_date[day]}')
            WHEN 1 THEN q1_amount
            WHEN 2 THEN q2_amount
            WHEN 3 THEN q3_amount
            WHEN 4 THEN q4_amount
        else 0.0
        end AS dealer_quarter_target
       ,q1.fin_year                                                                                     AS dt_year
       ,quarter('${sync_date[day]}')                          AS dt_quarter
FROM q1
LEFT JOIN a
ON q1.fin_year =a.bill_year
AND lower(q1.division)=lower(a.division)
AND lower(q1.sapi_d) = lower(a.customer_code)
AND lower(q1.dealer_type)=lower(a.cust_level3)
and a.bill_year is null 
union all
SELECT  a.division
       ,a.sub_division
       ,a.cust_level3
       ,a.bill_year
       ,quarter('${sync_date[day]}') as quarter
       ,a.dealer_name
       ,a.dealer_quar_complete
       ,case quarter('${sync_date[day]}')
            WHEN 1 THEN q1_amount
            WHEN 2 THEN q2_amount
            WHEN 3 THEN q3_amount
            WHEN 4 THEN q4_amount
        else 0.0
        end AS dealer_quarter_target
       ,a.bill_year                                                                                              AS dt_year
       ,quarter('${sync_date[day]}')                 AS dt_quarter
FROM a
LEFT JOIN q1
ON q1.fin_year =a.bill_year
AND lower(q1.division)=lower(a.division)
AND lower(q1.sub_buname) = lower(a.sub_division)
AND lower(q1.sapi_d) = lower(a.customer_code)
AND lower(q1.dealer_type)=lower(a.cust_level3)
where not (a.cust_level3='LP' and a.division in('IC','PION'))
union ALL 
select
        aa.division
       ,aa.sub_division
       ,aa.cust_level3
       ,aa.bill_year
       ,quarter('${sync_date[day]}')  as quarter
       ,aa.dealer_name
       ,aa.dealer_quar_complete
       ,case quarter('${sync_date[day]}')  
            WHEN 1 THEN q1_amount
            WHEN 2 THEN q2_amount
            WHEN 3 THEN q3_amount
            WHEN 4 THEN q4_amount
        else 0.0
        end AS dealer_quarter_target
       ,aa.bill_year                                                                                         AS dt_year
       ,quarter('${sync_date[day]}')                     AS dt_quarter
FROM aa
left JOIN q1
ON q1.fin_year =aa.bill_year
AND lower(q1.division)=lower(aa.division)
AND lower(q1.sapi_d) = lower(aa.customer_code)
AND lower(q1.dealer_type)=lower(aa.cust_level3)
where aa.cust_level3='LP' and aa.division in('IC','PION')
;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on  ${sync_date[year]} :${sync_date[month]}  .................."