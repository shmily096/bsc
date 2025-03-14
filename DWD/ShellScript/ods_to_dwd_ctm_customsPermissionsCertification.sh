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
insert overwrite table ${target_db_name}.dwd_ctm_CPC partition(dt='$sync_date')
select 
     Number
    ,Contract_No
    ,Application_No
    ,Category
    ,Category_Description
    ,Type
    ,Type_Description
    ,MAX_Usage
    ,Remaining_Usage
    ,StartDate
    ,StopDate
    ,DED_Type_Code
    ,DED_Type_Name
    ,Deduction_Type_Code
    ,Deduction_Type_Desc
    ,Domestic_Consignee_Code
    ,Domestic_Consignee_USCC
    ,Domestic_Consignor
    ,Owner_Code
    ,Owner_USCC
    ,Owner_Name
    ,Trade_Country
    ,Permit_Origin_Destination_Country
    ,Customs_Jurisdiction
    ,CIQ_Jurisdiction
    ,Destination
    ,Requestor
    ,Responsible_Person
    ,Veriifcation_Status_Code
    ,Veriifcation_Status_Desc
    ,Verification_Date
    ,Stop_Alarm
    ,Company_Transportation_Method
    ,Sequence_Number
    ,Part_Number
    ,Chinese_Description
    ,HS_Code
    ,Can_Use_Qty
    ,Use_Qty
    ,Remaining_Qty
    ,Customs_UM
    ,Available_Amount
    ,Used_Amount
    ,Remaining_Amount
    ,Currency
    ,Record_Net_Weight
    ,Used_NET_Weight
    ,Remain_Net_Weight
    ,Cost_Center
    ,Division
    ,Check_Flag
    ,Check_Number
    ,Check_Status
    ,Usage
    ,Part_Model
    ,Brand
    ,Origin_Destination_Country
from ${target_db_name}.ods_CTM_CPC
where   dt='$sync_date'
"
# 2. 执行SQL
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."