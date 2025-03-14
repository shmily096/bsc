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

if [ -n "$2" ] ;then 
    sync_year=$2
else
    sync_year=$(date  +'%Y')
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

-- sync up SQL string
insert overwrite table dwd_duty_by_upn partition(YEARMONTH)
select 
        CustomsDeclarationNo
        ,SeriaNumber
        ,TaxNumber
        ,ContractNo
        ,TaxBillCreateDT
        ,RecordArea
        ,Amount
        ,TaxPaymentDate
        ,MailDeliveryDate
        ,UPN
        ,Quantity
        ,BU
        , case when (DATE_FORMAT(taxbillcreatedt ,'yyyy-MM-dd') = '1900-01-01' or DATE_FORMAT(taxbillcreatedt,'yyyy-MM-dd') is null) then TaxPaymentDate
            else DATE_FORMAT(taxbillcreatedt ,'yyyy-MM-dd')  end as dt
        ,YEARMONTH
from ods_dutybyUPN   ---TRANS_DutybyUPN 杨帆手动上传
where   dt='$sync_date'
and YEARMONTH is not null
"
#每次更新当年的数据
# 2. 执行SQL
$hive -e "$so_sql"
hiveservices.sh start
echo "End syncing Sales order data into DWD layer on ${sync_date} .................."