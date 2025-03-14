#!/bin/bash
# Function:
#   sync up PG data to HDFS template
# History:
#   2022-03-31    slc   v1.0    init
# 下面四张表:  来源库名app_ops
# dbo.trans_inhospitalsalesall
# dbo.trans_lpcommercialsales
# dbo.trans_bsccommercialsalesall
# dbo.trans_salesdealerinventory  明细表
#变量1 源表名这里是
#变量2 源库名这里是tableaudb
#变量3分区时间  这个不填就是默认当天
# 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"
# 设置同步的数据库
sync_db='bsc_app_ops'
# 设置数据库连接字符串
connect_str_sqlserver="jdbc:sqlserver://10.226.99.103:16000;username=opsWin;password=opsWinZaq1@wsx;database=APP_OPS;"
connect_str_pg="jdbc:postgresql://10.226.98.58:55433/tableaudb"
user='postgres'
pwd='1qazxsw2'
# 同步日期设置，默认同步前一天数据
if [ -n "$2" ]; then
    sync_date=$2
else
    sync_date=$(date  +%F)
	
fi
yester_date1="$(date -d "1 day ago" +'%F')"
#同步SQl Server数据通过sqoop
sync_data_sqlserver(){
    echo "${sync_date} stat syncing........"
    hdfs dfs -mkdir -p /bsc/origin_data/$sync_db/$1/$sync_date
    $sqoop import \
        --connect "$connect_str_sqlserver" \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
        --num-mappers 1 \
        --fields-terminated-by '\t' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'
    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date
    echo "${sync_date} end syncing........"
}
sync_data_sqlserver_A(){
    echo "${sync_date} stat syncing........"
    hdfs dfs -mkdir -p /bsc/origin_data/$sync_db/$1/$sync_date
    $sqoop import \
        --connect "$connect_str_sqlserver" \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
        --num-mappers 1 \
        --fields-terminated-by ',' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'
    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date
    echo "${sync_date} end syncing........"
}
sync_data_sqlserver_B(){
    echo "${sync_date} stat syncing........"
    hdfs dfs -mkdir -p /bsc/origin_data/$sync_db/$1/$sync_date
    $sqoop import \
        --connect "$connect_str_sqlserver" \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
        --num-mappers 1 \
        --fields-terminated-by '\001' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'
    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/$sync_db/$1/$sync_date
    echo "${sync_date} end syncing........"
}

sync_sto_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
        --target-dir /bsc/origin_data/tableaudb/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
		--hive-drop-import-delims \
        --num-mappers 1 \
        --fields-terminated-by '\001' \
        --compress \
        --compression-codec lzop \
        --null-string '\\N' \
        --null-non-string '\\N'

    hadoop jar /opt/module/hadoop3/share/hadoop/common/hadoop-lzo-0.4.20.jar \
        com.hadoop.compression.lzo.DistributedLzoIndexer \
        /bsc/origin_data/tableaudb/$1/$sync_date

    echo "${sync_date} end syncing........"
}
# 同步库存销售数据
# 同步策略 - 全量
# dbo.TRANS_InHospitalSalesAll
# dbo.TRANS_LPCommercialSales
# dbo.TRANS_BSCCommercialSalesAll
# dbo.TRANS_SalesDealerInventory
sync_BSCCommercialSalesAll(){
    echo "Start syncing TRANS_BSCCommercialSalesAll data information"
    sync_data_sqlserver "trans_bsccommercialsalesall" "SELECT 
														LTRIM(RTRIM(Division))as Division
														,LTRIM(RTRIM(DivisionID))as DivisionID
														,LTRIM(RTRIM(SubBU))as SubBU
														,LTRIM(RTRIM(SAPID))as SAPID
														,LTRIM(RTRIM(DealerName))as DealerName
														,LTRIM(RTRIM(SOPType))as SOPType
														,LTRIM(RTRIM(SOPNumber))as SOPNumber
														,LTRIM(RTRIM(UPN))as UPN
														,LTRIM(RTRIM(Level5Desc))as Level5Desc
														,LTRIM(RTRIM(LOT))as LOT
														,LTRIM(RTRIM(EXPDate))as EXPDate
														,LTRIM(RTRIM(TransactionDate))as TransactionDate
														,LTRIM(RTRIM(QTY))as QTY
														,LTRIM(RTRIM(InvoicePrice))as InvoicePrice
														,LTRIM(RTRIM(BSCInvoiceAmtRMB))as BSCInvoiceAmtRMB
														,LTRIM(RTRIM(BSCInvoiceAmtUSD))as BSCInvoiceAmtUSD
														,LTRIM(RTRIM(MONTH))as MONTH
														,LTRIM(RTRIM(YEAR))as YEAR
														,LTRIM(RTRIM(DMSCode))as DMSCode
														,LTRIM(RTRIM(BICode))as BICode
														,LTRIM(RTRIM(BIName))as BIName
														,LTRIM(RTRIM(PONumber))as PONumber
														,LTRIM(RTRIM(ProductGroup))as ProductGroup
														,LTRIM(RTRIM(RSM))as RSM
														,LTRIM(RTRIM(ZSM))as ZSM
														,LTRIM(RTRIM(TSM))as TSM
														,LTRIM(RTRIM(Sales))as Sales
														,LTRIM(RTRIM(MarketType))as MarketType
														,LTRIM(RTRIM(Remark))as Remark
														,LTRIM(RTRIM(DeliveryNo))as DeliveryNo
														,LTRIM(RTRIM(CRMCode))as CRMCode
														,LTRIM(RTRIM(PCS))as PCS
														,LTRIM(RTRIM(InventoryChecker))as InventoryChecker
														,LTRIM(RTRIM(LastestInventoryChecker))as LastestInventoryChecker
														,LTRIM(RTRIM(SFDA))as SFDA
														,LTRIM(RTRIM(IsReturn))as IsReturn
														,LTRIM(RTRIM(TaxRate))as TaxRate
														,LTRIM(RTRIM(EquipmentFlag))as EquipmentFlag
														,LTRIM(RTRIM(DateOfManuf))as DateOfManuf
														,LTRIM(RTRIM(OrderReason))as OrderReason
														from dbo.trans_bsccommercialsalesall
														where 1=1"
    echo "End syncing sync_BSCCommercialSalesAll master data information"
}
# sync_salesdealerinventory() {
    # echo "Start syncing TRANS_SalesDealerInventory data information"
    # sync_data_sqlserver_A" trans_salesdealerinventory" "SELECT 
													#DivisionID
													#,OwnerID
													#,OwnerName
													#,OwnerParentSAPID
													#,OwnerParentDealerName
													#,SubBU
													#,OwnerType
													#,OwnerSalesType
													#,MarketType
													#,LocationID
													#,LocationName
													#,LocationDealerType
													#,LocationParentSAPID
													#,LocationParentDealer
													#,UPN
													#,CRMCode
													#,LOT
													#,EXPDate
													#,ExpYear
													#,ExpMonth
													#,Aging
													#,InvAmtByDearlerStdPurPrice
													#,BSCStdSellPrice
													#,QTY
													#,InvAmtByBSCStdSellPrice
													#,YEAR
													#,MONTH
													#,InventoryCategory
													#,InventoryTypeName
													#,BICode
													#,BIName
													#,FormNbr
													#,CustomizedGroup
													#,ProductGroup
													#,PCs
													#,QtyWithQR
													#,QtyWithNoQR
													#,WareHouse
													#,WareHouseType
													#,TransferDate
													#,TransferDays
													#,ReceiptDate
													#,ReceiptDays
													#,EquipFlag
													#,OrderType
													#,HospitalCode
													#,InventoryDate
													# FROM APP_OPS.dbo.TRANS_SalesDealerInventory
												# where 1=1"
    # echo "End syncing TRANS_SalesDealerInventory master data information"
# }

sync_InHospitalSalesAll() {
    echo "Start syncing TRANS_InHospitalSalesAll data information"
    sync_data_sqlserver_B "trans_inhospitalsalesall" "SELECT
													Division
													,DivisionID
													,SubBU
													,SAPID
													,ParentSAPID
													,ParentDealer
													,DMSCode
													,Hospital
													,RSM
													,ZSM
													,TSM
													,REP
													,UPN
													,LOT
													,ExpDate
													,QTY
													,BSCStdSellPrice
													,DealerStdPurPrice
													,HospitalPurPriceVAT
													,PurAmtRMBByBSCStdSellPrice
													,PurAmtUSDByBSCStdSellPrice
													,[MONTH]
													,[YEAR]
													,Province
													,City
													,Region
													,TransactionDate
													,Invoice
													,Invoicedate
													,SalesType
													,ProductShareType
													,BICode
													,BIName
													,AdjType
													,AdjReason
													,InputTime
													,HospitalMarketType
													,SPHShipmentNbr
													,CustomizedGroup
													,ProductGroup
													,StandardQuotaPrice
													,StandardQuotaAmount
													,Point
													,ActHSQuotaPriceBO
													,CRMCode
													,Remark
													,PCsNo
													,PONumber
													,QtyWithQR
													,QtyWithoutQR
													,QRSource
													,ActQuotaPrice
													,ActQuotaAmount
													,HospitalCategory
													,CurrentREP
													,CurrentZSM
													,CurrentTSM
													,CurrentRSM
													,EquipmentFlag
													,ConsignmentOrderType
													,WarehouseCode
													,warehousename
												FROM APP_OPS.dbo.TRANS_InHospitalSalesAll
												where 1=1"
    echo "End syncing plant master data information"
}
sync_LPCommercialSales() {
    echo "Start syncing TRANS_LPCommercialSales data information"
    sync_data_sqlserver_B "trans_lpcommercialsales" "SELECT 
													DivisionID
													,SubBU
													,SAPID
													,DealerName
													,DealerType
													,ParentSAPID
													,ParentDealer
													,RSM
													,ZSM
													,TSM
													,Sales
													,UPN
													,LOT
													,EXPDate
													,TransactionDate
													,SubmitDate
													,QTY
													,BSCStdSellPrice
													,T2ActPurPriceVAT
													,T2PurAmtByT2ActPurPriceVATRMB
													,T2PurAmtByT2ActPurPriceVATUSD
													,SalesType
													,ProductShareType
													,MarketType
													,MONTH
													,YEAR
													,NBR
													,BICode
													,BIName
													,AdjAmount
													,Remark
													,CustomizedGroup
													,ProductGroup
													,OrderNo
													,OrderType
													,CRMCode
													,FLKMDealer
													,PCS
													,QtyWithQR
													,QtyWithoutQR
													,InventoryChecker
													,LastestInventoryChecker
													,CoreRSM
													,SubBUCode
													,TaxRate
													,ProductEquipCatName
													,ConsignmentOrderType
													,RSMAlt
													,ZSMAlt
													FROM APP_OPS.dbo.TRANS_LPCommercialSales
												where 1=1"
    echo "End syncing plant master data information"
}
sync_TRANS_LPGReport() {
    echo "Start syncing TRANS_LPGReport data information"
    sync_data_sqlserver_B "TRANS_LPGReport" "SELECT 
														UpdateDT,
														UPN,
														Qty,
														LPGNumber, 
														Revision, 
														Description, 
														Status,
														SourceDoc,
														CFDARegNumber,
														PrimaryLabel, 
														SecondaryLabel,
														replace(LegalMfrSiteName,char(10),'') AS LegalMfrSiteName,
														LegalMfrSiteAddress,
														ManufacturingSiteAddress,
														LocalBusinessName,
														LocalBusinessAddress,
														CustomerServiceContactNumber,
														StandardNumber,
														RegistrationSTARTDate,
														RegistrationENDDate,
														DOMCheckRequired, 
														DualManufactured, 
														LocalBusinessNameOLD,
														LocalBusinessAddressOLD, 
														CustomerServiceContactNumberOLD, 
														PrintQuantitySecondaryLabel, 
														RegisteredProductName,
														ModelOrCatalogNumber,
														PrintModelOrCatalogNumber,
														SterilizationMethod,
														SuppressExpirationDate,
														SpecialRequirements,
														DFU, 
														GraphicFilename,
														CMIITID, 
														ElectricalSpecificationsCEOnly,
														SystemProductNameCEOnly,
														ServiceLifeCEOnly,
														ExpirationDateCEOnly,
														GTIN,
														ProdDesc
												FROM APP_OPS.dbo.TRANS_LPGReport
												where 1=1"
    echo "End syncing TRANS_LPGReport master data information"
}
sync_coo_manufacturemapping() {
    echo "Start syncing coo_manufacturemapping data information"
    sync_sto_pg "coo_manufacturemapping" "SELECT  coo, manufacturing_site_address, status,dt
								FROM public.coo_manufacturemapping
													where dt>=date('$sync_date')-1"
    echo "End syncing coo_manufacturemapping master data information"
}
if [ "$1"x = "trans_bsccommercialsalesall"x ];then
	echo "$1 only run"
	echo "  ok"
	sync_BSCCommercialSalesAll
elif [ "$1"x = "trans_salesdealerinventory"x ];then
    echo " $1 only run"
	echo "  ok"
	sync_salesdealerinventory
elif [ "$1"x = "trans_lpcommercialsales"x ];then
    echo " $1 only run"
	echo "  ok"
	sync_LPCommercialSales
elif [ "$1"x = "trans_inhospitalsalesall"x ];then
    echo " $1 only run"
	echo "  ok"	
	sync_InHospitalSalesAll
elif [ "$1"x = "TRANS_LPGReport"x ];then
    echo " $1 only run"
	echo "  ok"	
	sync_TRANS_LPGReport
	sync_coo_manufacturemapping
else
    echo "参数错误请重新输入"
    # sync_BSCCommercialSalesAll
	# sync_InHospitalSalesAll
	# sync_LPCommercialSales
	#sync_salesdealerinventory
fi
##############################################第二阶段hdfs to ods
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
echo "Start loading data on {$sync_date} .hdfs to ods................."
master_sql=""
bsccommercialsalesall="
load data inpath '/bsc/origin_data/$origin_db_name/trans_bsccommercialsalesall/$sync_date' overwrite
into table ${target_db_name}.ods_trans_bsccommercialsalesall
;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/trans_bsccommercialsalesall/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$bsccommercialsalesall"
fi
salesdealerinventory="
load data inpath '/bsc/origin_data/$origin_db_name/trans_salesdealerinventory/$sync_date' overwrite
into table ${target_db_name}.ods_trans_salesdealerinventory
;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/trans_salesdealerinventory/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$salesdealerinventory"
fi
trans_lpcommercialsales="
load data inpath '/bsc/origin_data/$origin_db_name/trans_lpcommercialsales/$sync_date' overwrite
into table ${target_db_name}.ods_trans_lpcommercialsales
;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/trans_lpcommercialsales/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$trans_lpcommercialsales"
fi
trans_inhospitalsalesall="
load data inpath '/bsc/origin_data/$origin_db_name/trans_inhospitalsalesall/$sync_date' overwrite
into table ${target_db_name}.ods_trans_inhospitalsalesall
;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/trans_inhospitalsalesall/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$trans_inhospitalsalesall"
fi
trans_lpgreport="
load data inpath '/bsc/origin_data/$origin_db_name/TRANS_LPGReport/$sync_date' overwrite
into table ${target_db_name}.ods_trans_lpgreport
;
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/TRANS_LPGReport/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$trans_lpgreport"
fi
ods_coo_manufacturemapping="
load data inpath '/bsc/origin_data/tableaudb/coo_manufacturemapping/$sync_date' overwrite
into table ${target_db_name}.ods_coo_manufacturemapping
;
"
hdfs dfs -test -d "/bsc/origin_data/tableaudb/coo_manufacturemapping/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$ods_coo_manufacturemapping"
fi
# 2. 执行加载数据SQL
$hive -e"$master_sql"
echo "End loading data on {$sync_date} ..hdfs to ods................"
##############################################第三阶段ods to dwd
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

export LANG="en_US.UTF-8"
echo "start syncing ods data into DWD layer on ${sync_date} .................."

dwd_trans_inhospitalsalesall_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
-- cfda master data
insert overwrite table "${target_db_name}".dwd_trans_inhospitalsalesall partition(year)
SELECT 
division
,divisionid
,subbu
,sapid
,parentsapid
,parentdealer
,dmscode
,hospital
,rsm
,zsm
,tsm
,rep
,upn
,lot
,expdate
,qty
,bscstdsellprice
,dealerstdpurprice
,hospitalpurpricevat
,puramtrmbbybscstdsellprice
,puramtusdbybscstdsellprice
,province
,city
,region
,transactiondate
,invoice
,invoicedate
,salestype
,productsharetype
,bicode
,biname
,adjtype
,adjreason
,inputtime
,hospitalmarkettype
,sphshipmentnbr
,customizedgroup
,productgroup
,standardquotaprice
,standardquotaamount
,point
,acthsquotapricebo
,crmcode
,remark
,pcsno
,ponumber
,qtywithqr
,qtywithoutqr
,qrsource
,actquotaprice
,actquotaamount
,hospitalcategory
,currentrep
,currentzsm
,currenttsm
,currentrsm
,equipmentflag
,consignmentordertype
,warehousecode
,warehousename
,month 
,year 
from "${target_db_name}".ods_trans_inhospitalsalesall   
where int(year)>1 ;
" 
dwd_trans_lpcommercialsales_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
-- cfda master data
insert overwrite table "${target_db_name}".dwd_trans_lpcommercialsales partition(year)
SELECT 
DivisionID
,SubBU
,SAPID
,DealerName
,DealerType
,ParentSAPID
,ParentDealer
,RSM
,ZSM
,TSM
,Sales
,UPN
,LOT
,EXPDate
,TransactionDate
,SubmitDate
,QTY
,BSCStdSellPrice
,T2ActPurPriceVAT
,T2PurAmtByT2ActPurPriceVATRMB
,T2PurAmtByT2ActPurPriceVATUSD
,SalesType
,ProductShareType
,MarketType
,NBR
,BICode
,BIName
,AdjAmount
,Remark
,CustomizedGroup
,ProductGroup
,OrderNo
,OrderType
,CRMCode
,FLKMDealer
,PCS
,QtyWithQR
,QtyWithoutQR
,InventoryChecker
,LastestInventoryChecker
,CoreRSM
,SubBUCode
,TaxRate
,ProductEquipCatName
,ConsignmentOrderType
,RSMAlt
,ZSMAlt
,month
,year
from "${target_db_name}".ods_trans_lpcommercialsales   
where int(year)>1 ;
"
dwd_trans_bsccommercialsalesall_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
#set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
-- cfda master data
insert overwrite table "${target_db_name}".dwd_trans_bsccommercialsalesall partition(year)
SELECT 
Division
,DivisionID
,SubBU
,SAPID
,DealerName
,SOPType
,SOPNumber
,UPN
,Level5Desc
,LOT
,EXPDate
,TransactionDate
,QTY
,InvoicePrice
,BSCInvoiceAmtRMB
,BSCInvoiceAmtUSD
,DMSCode
,BICode
,BIName
,PONumber
,ProductGroup
,RSM
,ZSM
,TSM
,Sales
,MarketType
,Remark
,DeliveryNo
,CRMCode
,PCS
,InventoryChecker
,LastestInventoryChecker
,SFDA
,IsReturn
,TaxRate
,EquipmentFlag
,DateOfManuf
,OrderReason
,month
,year
from "${target_db_name}".ods_trans_bsccommercialsalesall   
where int(year)>1 ;"

dwd_trans_salesdealerinventory_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
-- cfda master data
insert overwrite table "${target_db_name}".dwd_trans_salesdealerinventory partition(year)
SELECT 
DivisionID
,OwnerID
,OwnerName
,OwnerParentSAPID
,OwnerParentDealerName
,SubBU
,OwnerType
,OwnerSalesType
,MarketType
,LocationID
,LocationName
,LocationDealerType
,LocationParentSAPID
,LocationParentDealer
,UPN
,CRMCode
,LOT
,EXPDate
,ExpYear
,ExpMonth
,Aging
,InvAmtByDearlerStdPurPrice
,BSCStdSellPrice
,QTY
,InvAmtByBSCStdSellPrice
,YEAR
,MONTH
,InventoryCategory
,InventoryTypeName
,BICode
,BIName
,FormNbr
,CustomizedGroup
,ProductGroup
,PCs
,QtyWithQR
,QtyWithNoQR
,WareHouse
,WareHouseType
,TransferDate
,TransferDays
,ReceiptDate
,ReceiptDays
,EquipFlag
,OrderType
,HospitalCode
,InventoryDate
,month
,year
from "${target_db_name}".ods_trans_salesdealerinventory   
where int(year)>1 ;
"
dwd_ods_trans_lpgreport_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
-- cfda master data
insert overwrite table "${target_db_name}".dwd_ods_trans_lpgreport partition(dt)
SELECT 
updatedt, 
upn, 
qty, 
lpgnumber, 
revision, 
description,
 status, 
 sourcedoc, cfdaregnumber, primarylabel, secondarylabel, legalmfrsitename,
  legalmfrsiteaddress, manufacturingsiteaddress, localbusinessname,
   localbusinessaddress, customerservicecontactnumber, standardnumber, 
   registrationstartdate, registrationenddate, domcheckrequired,
    dualmanufactured, localbusinessnameold, localbusinessaddressold,
	 customerservicecontactnumberold, printquantitysecondarylabel,
	  registeredproductname, modelorcatalognumber, printmodelorcatalognumber,
	   sterilizationmethod, suppressexpirationdate, specialrequirements, dfu,
	    graphicfilename, cmiitid, electricalspecificationsceonly, systemproductnameceonly,
		 servicelifeceonly, expirationdateceonly, gtin, proddesc, 
		 substring(updatedt,1,10) as dt
from "${target_db_name}".ods_trans_lpgreport   
where int(substring(updatedt,1,1))>1 ;
"
dwd_ods_coo_manufacturemapping_sql="
use "${target_db_name}";
-- 参数
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.reducers.max=8; 
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;	
-- cfda master data
insert overwrite table "${target_db_name}".dwd_ods_coo_manufacturemapping partition(dt)
SELECT 
coo,
manufacturing_site_address,
status,
dt
from "${target_db_name}".ods_coo_manufacturemapping   
where status is not null ;
"
# 2. 执行加载数据SQL
if [ "$1"x = "trans_bsccommercialsalesall"x ];then
	echo "$1 only run"
	echo "  ok"
	"$hive" -e "$dwd_trans_bsccommercialsalesall_sql"
elif [ "$1"x = "trans_salesdealerinventory"x ];then
    echo " $1 only run"
	echo "  ok"
	#"$hive" -e "$dwd_trans_salesdealerinventory_sql"
elif [ "$1"x = "trans_lpcommercialsales"x ];then
    echo " $1 only run"
	echo "  ok"
	"$hive" -e "$dwd_trans_lpcommercialsales_sql"
elif [ "$1"x = "trans_inhospitalsalesall"x ];then
    echo " $1 only run"
	echo "  ok"
	"$hive" -e "$dwd_trans_inhospitalsalesall_sql"
elif [ "$1"x = "TRANS_LPGReport"x ];then
    echo " $1 only run"
	echo "  ok"
	"$hive" -e "$dwd_ods_trans_lpgreport_sql"
	"$hive" -e "$dwd_ods_coo_manufacturemapping_sql"
	dwd_ods_trans_lpgreport_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_ods_trans_lpgreport | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
	dwd_ods_coo_manufacturemapping_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_ods_coo_manufacturemapping | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
	dwd_trans_lpgreport_sql="
		use "${target_db_name}";
		-- 参数
		set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
		set hive.exec.reducers.max=8; 
		set mapred.reduce.tasks=8;
		set hive.exec.parallel=false;
		set hive.exec.dynamic.partition=true;
		set hive.exec.dynamic.partition.mode=nostrict;	
		-- cfda master data
		insert overwrite table "${target_db_name}".dwd_trans_lpgreport
		SELECT updatedt, upn, qty, lpgnumber, revision, description, a.status, sourcedoc, cfdaregnumber, primarylabel, 
			secondarylabel, legalmfrsitename, legalmfrsiteaddress, manufacturingsiteaddress, localbusinessname, 
			localbusinessaddress, customerservicecontactnumber, standardnumber, registrationstartdate, 
			registrationenddate, domcheckrequired, dualmanufactured, localbusinessnameold, localbusinessaddressold, 
			customerservicecontactnumberold, printquantitysecondarylabel, registeredproductname, modelorcatalognumber, 
			printmodelorcatalognumber, sterilizationmethod, suppressexpirationdate, specialrequirements, dfu, graphicfilename,
			cmiitid, electricalspecificationsceonly, systemproductnameceonly, servicelifeceonly, expirationdateceonly,
			gtin, a.proddesc,
			if(b.coo='',null,b.coo) as coo,
			b.status as coo_status,a.dt
		FROM (select * from opsdw.dwd_ods_trans_lpgreport where dt='$dwd_ods_trans_lpgreport_maxdt' and status='Active') a
		left join (select * from opsdw.dwd_ods_coo_manufacturemapping  where dt='$dwd_ods_coo_manufacturemapping_maxdt') b 
			on a.manufacturingsiteaddress=b.manufacturing_site_address
		;
		"
	echo "$dwd_trans_lpgreport_sql"
	"$hive" -e "$dwd_trans_lpgreport_sql"
	sh /bscflow/ods/remove_ods_trans_lpgreport.sh
	sh /bscflow/ods/remove_demo.sh /bsc/opsdw/dwd/dwd_ods_coo_manufacturemapping
else
    echo "参数错误请重新输入"
    # "$hive" -e "$dwd_trans_inhospitalsalesall_sql"
	# echo "dwd_trans_inhospitalsalesall   is  finish"
	# "$hive" -e "$dwd_trans_lpcommercialsales_sql"
	# echo "dwd_trans_lpcommercialsales   is  finish"
	# "$hive" -e "$dwd_trans_bsccommercialsalesall_sql"
	# echo "dwd_trans_bsccommercialsalesall   is  finish"
	#"$hive" -e "$dwd_trans_salesdealerinventory_sql"
fi
echo "End syncing ods data into DWD layer on ${sync_date} .................."