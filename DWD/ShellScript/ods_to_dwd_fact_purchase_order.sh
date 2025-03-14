#!/bin/bash
# Function:
#   sync up po data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init
# 2021-05-24    Donny   v1.1    add clean rule

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing domestic sto data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
--set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

drop table if exists tmp_dwd_fact_purchase_order_info_po;
create table tmp_dwd_fact_purchase_order_info_po stored as orc as
    select purchase_order_no
       ,po_create_dt
       ,po_create_by
       ,po_updated_dt
       ,po_updated_by
       ,po_status
       ,po_line_no
       ,material
       ,qty
       ,unit
       ,purchase_price
       ,currency
       ,migo_date
       ,batch
       ,received_qty 
    from  ${target_db_name}.ods_purchase_order --源表TRANS_ThirdPartyPurchaseOrder增量
    where dt='$sync_date' and month(update_dt)>1;

drop table if exists tmp_dwd_fact_purchase_order_info_sku;
create table tmp_dwd_fact_purchase_order_info_sku stored as orc as
    select material, 
        COALESCE(standard_cost, 1.0) unit_price, 
        division 
        from  ${target_db_name}.ods_material_master --源表MDM_MaterialMaster全量
        where dt='$sync_date';

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_purchase_order_info partition(dt='$sync_date')
select  po.purchase_order_no --purch_id
       ,po.po_status
       ,po.po_create_dt --created_datetime
       ,po.po_updated_dt --updated_datetime
       ,po.po_create_by --created_by
       ,po.po_updated_by --updated_by
       ,po.po_line_no -- line_number
       ,po.material
       ,po.qty
       ,po.received_qty
       ,nvl(po.unit,'e')
       ,po.purchase_price --purch_price
       ,po.currency 
       ,po.migo_date
       ,null
       ,null
       ,sku.division --financial_dimension_id
from tmp_dwd_fact_purchase_order_info_po po
left outer join tmp_dwd_fact_purchase_order_info_sku sku on po.material=sku.material; 
"
delete_tmp="
drop table tmp_dwd_fact_purchase_order_info_po;
drop table tmp_dwd_fact_purchase_order_info_sku;
"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"
$hive -e "$delete_tmp"
echo "End syncing po data into DWD layer on ${sync_date} .................."