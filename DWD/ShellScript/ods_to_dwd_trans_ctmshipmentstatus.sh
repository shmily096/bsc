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
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
--set hive.exec.reducers.max=8; 
--set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- sync up SQL string
insert overwrite table dwd_trans_ctmshipmentstatus partition(dt='$sync_date')
select 
    id
    ,updatedt
    ,active
    ,worknumber
    ,trim(invoice) as commercialinvoice
    ,bscinformslcdate
    ,t1pickupdate
    ,actualarrivaltime
    ,dockwarrantdate
    ,forwordinginformslcpick
    ,forwording
    ,intoinventorydate
    ,updatedate
    ,shipmentinternalnumber
    ,masterbillno
    ,housewaybillno
    ,importexportflag
    ,shipmenttype
    ,emergencysigns
    ,merchandiser
    ,vouchermaker
    ,abnormalcauses1
    ,abnormalcauses2
    ,inspectionmark1
    ,inspectionmark2
    ,inspectionmark3
    ,remark
    ,quantity
    ,grossweight
    ,forwarderservicelevel
    ,department
    ,countryarea
    ,transportationtype
    ,customssupervisioncertificate
    ,commodityinspectiondemand
    ,customizedcertificate
    ,etd
    ,eta
    ,reviseetd
    ,reviseeta
    ,commodityinspection
    ,customsinspection
    ,declarationcompletiondate
    ,commericalinvoce
    ,draftcompletiondate
    ,edireleasedate
    ,taxpaymentcompletiondate
    ,customsclarancecomplete
    ,inspectionquarantinecertificatecompletedate
    ,pickupdocumentsrecieveddate
    ,preinspectionstartdate
    ,customsclearanceorderreceived
    ,inspectionslipreceived
    ,workcontactlistcomplete
    ,testdate
    ,ciqcomplete
    ,inspectionstarttime
    ,distributionlistreceiveddate
    ,infotype
from ods_trans_ctmshipmentstatus ---暂时是全量更新因为数据量太少 源表：TRANS_CTMShipmentStatus
lateral view explode(split(replace(replace(commercialinvoice,',','/'),'，','/'),'/')) t as invoice
where dt='$sync_date' 
and infotype is not null;"
# 2. 执行SQL

$hive -e "$so_sql"
sh /bscflow/dwd/ods_to_dwd_iekpi_e2e.sh
sh /bscflow/dws/dwd_to_dws_iekpi_e2e.sh
sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dws_iekpi_e2e
sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dws_iekpi_e2e
sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh outbound_distribution
echo "End syncing Sales order data into DWD layer on ${sync_date}  .................."