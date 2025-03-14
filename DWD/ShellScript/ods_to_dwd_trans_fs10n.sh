#!/bin/bash
# Function:
#   sync up sales order data from ods to dwd layer
# History:
# 2021-11-16    Amanda   v1.0    init


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

echo "start syncing so into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwd_fact_trans_fs10ndetail partition(yearmonth)
select 
    updatedt, 
    documentno,
    pstngdate, 
    lcurr,
    pk,
    typ, 
    profitctr, 
    itm, 
    account, 
    text,
    username,
    docdate,
    amountindoccurr,
    curr,
    amountinlocalcur,
    material,
    costctr, 
    yearmonth
from ods_trans_fs10ndetail  --TRANS_FS10NDetail 全量
where acitve='1';
-- dwd_trans_fs10n sync up SQL string
insert overwrite table dwd_fact_trans_fs10n 
select 
    updatedt, 
    period, 
    debit, 
    credit, 
    balance, 
    cumbalance, 
    year
from ods_trans_fs10n  ---TRANS_FS10N 全量
where active='1';
"
# 2. 执行SQL
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."