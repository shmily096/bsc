#!/bin/bash
# Function:
#   sync up dealer purchase quotation data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

# 参数
target_db_name='opsdw'                # 目标数据库名称
hive=/opt/module/hive3/bin/hive       # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天
if [ -n "$1" ]; then
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dealer purchase quotation data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
----set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_dealer_purchase_quotation partition(dt='$sync_date')
select  
        fin_year
       ,case when division='CRM' then 'CRM'
             when division='Cardio' then 'IC'
             when division='EP' then 'EP'
             when division='Endo' then 'ENDO'
             when division='NM' then 'NM'
             when division='PI' then 'PI'
             when division='Pulmonary' then 'PUL'
             when division='SH' then 'SH'
             when division='Uro' then 'URO'
             when division='LT' then 'LT'
             when division='LTS' then 'LTS'
             when division='IO' then 'IO'
       else division
       end as division
       ,sub_buname
       ,sapi_d
       ,dealer_name
       ,parent_sapid
       ,parent_dealer_name
       ,dealer_type
       ,rsm
       ,zsm
       ,tsm
       ,contract_start_date
       ,contract_end_date
       ,market_type
       ,contract_status
       ,new_old_dealer_by_bu
       ,new_old_dealer_by_bsc
       ,aop_type
       ,month1_amount
       ,month2_amount
       ,month3_amount
       ,q1_amount
       ,month4_amount
       ,month5_amount
       ,month6_amount
       ,q2_amount
       ,month7_amount
       ,month8_amount
       ,month9_amount
       ,q3_amount
       ,month10_amount
       ,month11_amount
       ,month12_amount
       ,q4_amount
       ,year_total_amount
       ,bi_code
       ,bi_name
    from ${target_db_name}.ods_dealer_purchase_quotation --源表TRANS_QuotaDealerPurchase 全量
    where dt = '$sync_date'
    and substr(fin_year,1,1)='2'
"
# 2. 执行加载数据SQL
#2022-08-18本次修改去掉了group by去重,取最大分区直接改为取当天的分区,提高脚本执行速度
echo "$sto_sql"
$hive -e "$sto_sql"

echo "End syncing dealer purchase quotation data into DWD layer on ${sync_date} .................."
