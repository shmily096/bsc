#!/bin/bash
# Function:
#   sync up dws_dsr_daily_trans 
# History:
# 2021-06-29    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date -d '-1 day' +%F)
fi

sync_year=${sync_date:0:4}
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing dws_dsr_daily_trans data into DWS layer on $sync_year : $sync_date .................."


sql_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;

--with parts
--可履行金额, by onhand_dat e
With A AS (
       SELECT  DATE_FORMAT(onhand_date,'yyyy-MM-dd') AS onhand_date
              ,division
              ,SUM(total_value)                      AS fulfilled_value
       FROM 
       (
              SELECT  onhand_date
                     ,division
                     ,material
                     ,SUM(total_value) total_value
              FROM dws_dsr_fulfill_daily_trans
              GROUP BY  onhand_date
                     ,division
                     ,material 
       )a
       GROUP BY  DATE_FORMAT(onhand_date,'yyyy-MM-dd')
              ,division 
),
--实际发货，退货，正在发货金额 by bill_date 
B AS (
       SELECT  DATE_FORMAT(bill_date,'yyyy-MM-dd') bill_date 
              ,division
              ,SUM(net_amount_return) net_amount_return
              ,SUM(net_amount_dn) net_amount_dn
              ,SUM(net_amount_shiped) net_amount_shiped
              ,SUM(net_amount_without_invoice) net_amount_without_invoice
              ,SUM(net_amount_invoice) net_amount_invoice
              ,SUM(net_cr_shiped) net_cr_shiped
       FROM dws_dsr_ship_daily_trans
       GROUP BY  DATE_FORMAT(bill_date,'yyyy-MM-dd')
              ,division 
)
--计算返利金额
,C AS (
       SELECT  DATE_FORMAT(fso.bill_date,'yyyy-MM-dd') bill_date
              ,dm.division_display_name AS division
              ,SUM(net_amount * soi.rebate_rate) rebate_value
       FROM dwd_fact_sales_order_invoice fso
       LEFT JOIN 
       (
              SELECT  material_code
                     ,division_display_name
              FROM dwd_dim_material 
              where dt='$dwd_dim_material_maxdt'
              ---in (select max(dt) from dwd_dim_material where dt>=date_sub('$sync_date',10))
       ) dm
       ON dm.material_code =fso.material
       LEFT JOIN 
       (
              SELECT  so_no
                     ,material
                     ,rebate_rate
              FROM dwd_fact_sales_order_info
       ) soi ON soi.so_no = fso.sales_id AND soi.material = fso.material
       WHERE sold_to != '554263' 
              or sold_to != '107352' 
              or SUBSTRING(fso.material, 1, 3) != 'RBT' 
              or LOWER(SUBSTRING(fso.purchase_order, 1, 5)) = 'price' 
              or fso.material not IN ('PD525', 'PD275', 'PD875') 
       GROUP BY  DATE_FORMAT(fso.bill_date,'yyyy-MM-dd')
              ,dm.division_display_name 
)

INSERT OVERWRITE TABLE ${target_db_name}.dws_dsr_daily_trans partition(dt)
SELECT  B.bill_date
       ,ucase(B.division)                     AS division
       ,SUM(A.fulfilled_value)                AS fulfilled_value
       ,SUM(B.net_amount_return)              AS net_amount_return
       ,SUM(B.net_amount_dn)                  AS net_amount_dn
       ,SUM(B.net_amount_shiped)              AS net_amount_shiped
       ,SUM(B.net_amount_without_invoice)     AS net_amount_without_invoice
       ,SUM(B.net_amount_invoice)             AS net_amount_invoice
       ,SUM(B.net_cr_shiped)                  AS net_cr_shiped
       ,SUM(C.rebate_value)                   AS rebate_value
       ,date_format(B.bill_date,'yyyy-MM-dd') AS dt_partition
FROM B
LEFT JOIN C
ON C.bill_date = B.bill_date AND ucase(C.division) = ucase(B.division)
LEFT JOIN A
ON A.onhand_date = B.bill_date AND ucase(A.division) = ucase(B.division)
WHERE year(B.bill_date) = '$sync_year'
GROUP BY  ucase(B.division)
         ,B.bill_date
;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing dws_dsr_daily_trans data into DWS layer on $sync_year : $sync_date .................."
