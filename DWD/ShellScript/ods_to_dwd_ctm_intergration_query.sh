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
insert overwrite table dwd_ctm_intergrationquery partition(dt)
select 
updatedt
,active
,costcenter
,divisionnumber
,forwarder_referenceid
,shipment_number
,blno
,shipmentcreatedate
,internalccsno
,ccsno
,hbno
,registereditemid
,importexport
,transportationmethod
,trademode
,tradingcountryregion
,incoterms
,ccscreatedate
,importexportdate
,declarationdate
,entryoperator
,trackingoperator
,commercialinvoiceno
,commercialinvoicesequencenumber
,customsmanifestdetailno
,ccsitemnumber
,customsitemno
,partnumber
,hscode
,chinesedescription
,englishpart
,bomversion
,descriptionbondedflag
,origincountry
,destinationcountryregion
,origindestinationcountry
,declqty
,customsum
,declaredunitprice
,currency
,declarationprice
,totalamount
,customsbomversion
,qty1
,unit1
,declareprice1
,qty2
,unit2
,senderreceiver
,netweight
,projectnumber
,ponumber
,dpp
,countrytax
,provisionaltax
,customsdutyrate
,actualtaxrate
,protocoltype
,vatrate
,consumptiontaxrate
,estimatedcustomsduty
,estimatedvat
,estimatedconsumptiontax
,mofcomapprovalnumber
,itemgroupid
,voyagenumber
,usagetype
,usagecode
,shipmenttype
,forwarder
,broker
,declaremode
,billto
,hblhawb
,mblmawb
,planner
,registrationnumber
,trackingid
,site
,tradingcompanycode
,tradingcompanyname
,receivercompanycode
,receivercompanyname
,financeinvoicenumber
,posequence
,sidremark
,brokerpoanumber
,commercialinvoiceprice
,commercialinvoicecurrency
,bondedmanifest
,bondedmanifestdeclaredate
,bondedmanifestdumark
,bmcustomsdeclarationsign
,bmcustomsdeclarationtype
,relationccs
,declarationform
,listingnumber
,antidumpingduty
,contractno
,upn
,date_format(CCSCreateDate,'yyyy-MM-dd') as dt
from ods_ctm_intergrationquery  --源表:TRANS_CTMIntegrationQuery 每次取最近3天的updat
where   dt='$sync_date'
and substr(CCSCreateDate,1,1)='2';
"
# 2. 执行SQL
$hive -e "$so_sql"

echo "End syncing Sales order data into DWD layer on ${sync_date} .................."