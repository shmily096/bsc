#!/bin/bash
# Function:
#   sync up inventory movement transation data to dwd layer
# History:
# 2021-05-18    Donny   v1.0    init

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

echo "start syncing inventory movement transation data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_inventory_movement_trans partition(dt)
select  
		mov.movement_type
       ,mov.reason_code
       ,mov.special_stock
       ,mov.material_doc
       ,mov.mat_item
       ,mov.stock_location
       ,mov.plant
       ,mov.material
       ,mov.batch
       ,mov.qty
       ,mov.sle_dbbd
       ,mov.posting_date
       ,mov.mov_time
       ,mov.user_name
       ,mov.delivery_no
       ,mov.po_number
       ,mov.po_item
       ,mov.header_text
       ,mov.original_reference
       ,mov.update_dt as enter_date
       ,date_format(mov.enter_date,'yyyy-MM-dd') as dt
from ${target_db_name}.ods_inventory_movement_trans mov
where dt='${sync_date}' and MONTH(enter_date)>=1;
---源表TRANS_InventoryTransactions 每次取当天的updatedt,对应的是最近3天的enterdate
---这个是之前为了防止sqlserver更新失败,所以和sqlserver对比数据的时候需要对sqlserver去重
--去重是在到hdfs这一步完成
---enter_date还是美国时间，updatedt是转换成中国时间进来的

"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"

echo "End syncing inventory movement transation data into DWD layer on ${sync_date} .................."