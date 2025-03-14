#!/bin/bash
#   sync up BSC APP data to HDFS
# History:
#   2021-05-07    Donny   v1.0    draft
#   2021-05-10    Donny   v1.1    update connection string & other table sync

# 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 设置同步的数据库
sync_db='bsc_app_ops'

# 设置数据库连接字符串
connect_str_mysql="jdbc:mysql://172.25.48.1:3306/$sync_db"
connect_str_sqlserver="jdbc:sqlserver://10.226.99.103:16000;username=opsWin;password=opsWinZaq1@wsx;database=APP_OPS;"

# 同步日期设置，默认同步前一天数据
if [ -n "$2" ]; then
    sync_date=$2
else
    sync_date=$(date  +%F)
fi
yesterday=$(date -d '-1 day' +%F)
this_year=`date -d "${sync_date}" +%Y-01`

# 获取当前日期  
current_date=$(date +"%Y-%m-%d")  
  
# 获取上个月的日期（减去一个月）  
last_month_date=$(date -d "$current_date -1 month" +"%Y-%m") 

#last_month_date='2024-05'

#同步SQl Server数据通过sqoop
sync_data_sqlserver() {
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
# 同步寄售csgn
sync_ods_trans_csgn_order() {
    sync_data_sqlserver "ods_trans_csgn_order" "select 
                                                ID ,
                                                UpdateDT ,
                                                Active ,
                                                OrderType ,
                                                SAPCode ,
                                                DealerName ,
                                                OrderStatus ,
                                                CreateDate ,
                                                ProductLineName ,
                                                UPN ,
                                                UPNName ,
                                                RequiredQty ,
                                                CFNPrice ,
                                                Amount 
                                                from APP_OPS.dbo.TRANS_Consignment_ClearBorrowDraftOrder
                                                where 1=1"
											}
sync_ods_mdm_materialmaster() {
    sync_data_sqlserver "ods_mdm_materialmaster" "select 
                                                  chinesename ,
                                                  material ,
                                                  oldcode ,
                                                  englishname ,
                                                  profitcenter ,
                                                  deliveryplant ,
                                                  standardcost,
                                                  transferprice,
                                                  materialtype,
                                                  standardcostusd,
                                                  division,
                                                  productmodel,
                                                  salesstatus
                                                  from APP_OPS.dbo.MDM_MaterialMaster
                                                  where 1=1"
											}

# 同步寄售t2
sync_ods_trans_csgn_t2() {
    sync_data_sqlserver "ods_trans_csgn_t2" "select 
  	                                            id, 
  	                                            updatedt, 
  	                                            active, 
  	                                            dealercode, 
  	                                            dealername, 
  	                                            dealertype, 
  	                                            nbr, 
  	                                            salesdate, 
                                                divisionid, 
                                                divisionname, 
                                                upn, 
                                                batch, 
                                                qrcode, 
                                                qty, 
                                                unitprice, 
                                                consignmenttype, 
                                                ordertype, 
                                                parentdealercode, 
                                                parentdealername
                                                from APP_OPS.dbo.trans_consignmenttransfert2
                                                where 1=1"
											}


# 同步salesdealer
sync_ods_trans_salesdealerinventory() {
    sync_data_sqlserver "ods_trans_salesdealerinventory" "select 
  	                                                        divisionid, 
                                                            ownerid, 
                                                            ownername, 
                                                            ownerparentsapid, 
                                                            ownerparentdealername, 
                                                            subbu, 
                                                            ownertype, 
                                                            ownersalestype, 
                                                            markettype, 
                                                            locationid, 
                                                            locationname, 
                                                            locationdealertype, 
                                                            locationparentsapid, 
                                                            locationparentdealer ,
                                                            upn, 
                                                            crmcode, 
                                                            lot, 
                                                            expdate, 
                                                            expyear, 
                                                            expmonth, 
                                                            aging, 
                                                            invamtbydearlerstdpurprice, 
                                                            bscstdsellprice, 
                                                            qty, 
                                                            invamtbybscstdsellprice, 
                                                            year, 
                                                            month, 
                                                            inventorycategory, 
                                                            inventorytypename, 
                                                            bicode, 
                                                            biname, 
                                                            formnbr, 
                                                            customizedgroup, 
                                                            productgroup, 
                                                            pcs, 
                                                            qtywithqr, 
                                                            qtywithnoqr, 
                                                            warehouse, 
                                                            warehousetype, 
                                                            transferdate, 
                                                            transferdays, 
                                                            receiptdate, 
                                                            receiptdays, 
                                                            equipflag, 
                                                            ordertype, 
                                                            hospitalcode, 
                                                            inventorydate,
                                                            ContractStatus, 
                                                            InventoryCategory2, 
                                                            AgingCategory, 
                                                            InventoryL3
                                                            FROM APP_OPS.dbo.TRANS_SalesDealerInventory 
                                                            where 1=1 and
                                                            CAST(year AS VARCHAR(4)) + '-' +   RIGHT('0' + CAST(month AS VARCHAR(2)), 2)='$last_month_date' 
                                                            "
											}

# TRANS_ConsignmentTracking
sync_ods_trans_consignmenttracking() {
    sync_data_sqlserver "ods_trans_consignmenttracking" "select 
  	                                            ID ,
                                                UpdateDT ,
                                                Active ,
                                                DivisionName ,
                                                CustomerCodeSAP ,
                                                CustomerNameSAP ,
                                                Customer ,
                                                CustomerNumber,
                                                Material ,
                                                Plant ,
                                                MaterialDescription ,
                                                Batch ,
                                                Expiration,
                                                Available ,
                                                [COMMITTED] ,
                                                DeliveryDocNum ,
                                                PostingDate ,
                                                SalesOrder ,
                                                OrderDate ,
                                                CustomerPONumber ,
                                                StorageType ,
                                                OrderType ,
                                                CustomerStatus ,
                                                MaterialType ,
                                                ConsignmentType ,
                                                DueDate ,
                                                RemainingDays ,
                                                PGIvsExpiration ,
                                                PGIvsExpirationScope ,
                                                ExpirationvsReportDate ,
                                                ExpirationvsReportDateScope ,
                                                LastWorkDate ,
                                                AlertMessenger 
                                                from APP_OPS.dbo.TRANS_ConsignmentTracking
                                                where 1=1 and
                                                CustomerNameSAP is not NULL and UpdateDT >'2024-01-30'"
											}


# TRANS_ConsignmentList
sync_ods_trans_consignmentlist() {
    sync_data_sqlserver "ods_trans_consignmentlist" "select 
  	                                            DataID, 
                                                UpdateDT, 
                                                DivisionID, 
                                                Division, 
                                                CustomerSite, 
                                                CustomerNumber, 
                                                CustomerName, 
                                                Material, 
                                                MaterialDescription, 
                                                ProductLineID2, 
                                                ProductLine2, 
                                                ProductLineID3, 
                                                ProductLine3, 
                                                ProductLineID4, 
                                                ProductLine4, 
                                                Batch, 
                                                ExpirationDate, 
                                                Quantity, 
                                                PurchasePrice, 
                                                Amount, 
                                                Category
                                                FROM APP_OPS.dbo.TRANS_ConsignmentList 
                                                where 1=1"
											}

# ods_kpi_complaint
sync_ods_kpi_complaint() {
    sync_data_sqlserver "ods_kpi_complaint" "select 
  	                                            ClosedYearMonth,
                                                DMSComplainNbr  ,
                                                Division ,
                                                QAPendingDays ,
                                                BUPendingDays ,
                                                CCPendingDays  
                                                from APP_OPS.dbo.TRANS_ComplaintCycleTime 
                                                where 1=1
                                                and ClosedYearMonth >'202312'"
											}

# ods_mdm_materialmaster_marc
sync_ods_mdm_materialmaster_marc() {
    sync_data_sqlserver "ods_mdm_materialmaster_marc" "select 
  	                                            Material, 
                                                Plant, 
                                                CommImpCode, 
                                                ProfitCenter, 
                                                LoadingGroup, 
                                                AvailCheck, 
                                                SerialNoProfile, 
                                                SourceList, 
                                                PostToInspStk, 
                                                StorageLocationEP, 
                                                PSMatlStatus, 
                                                LotSize, 
                                                SpecProcurement, 
                                                Procurement, 
                                                MinLotSze, 
                                                MaxLotSize, 
                                                RoundingValue, 
                                                FixedLotSize, 
                                                PlDeliveryTime, 
                                                GRProcTime, 
                                                SpecProcType, 
                                                MRPType, 
                                                ReorderPoint, 
                                                PurchGroup, 
                                                ControlCode, 
                                                LogisticsGroup, 
                                                UpdateDT
                                                FROM APP_OPS.dbo.MDM_MaterialMaster_MARC 
                                                where 1=1"
											}


# ods_trans_twinventory_consignment
sync_ods_trans_twinventory_consignment() {
    sync_data_sqlserver "ods_trans_twinventory_consignment" "select 
  	                                            DataID, 
                                                UpdateDT, 
                                                ID, 
                                                Material, 
                                                Customer, 
                                                Plant, 
                                                Batch, 
                                                CustomerNumber, 
                                                City, 
                                                MaterialDescription, 
                                                Expiration, 
                                                Available, 
                                                [Committed], 
                                                [Type], 
                                                DeliveryDocNum, 
                                                Remito, 
                                                [Posting Date], 
                                                SalesOrder, 
                                                OrderDate, 
                                                MaterialDocument, 
                                                CustomerPONumber, 
                                                CustomerMaterialNum, 
                                                NotaFiscal, 
                                                BatchDate, 
                                                SerNr
                                                FROM APP_OPS.dbo.TRANS_TWInventory_Consignment 
                                                where 1=1"
											}

# ods_mdm_upn_mbew
sync_ods_mdm_upn_mbew() {
    sync_data_sqlserver "ods_mdm_upn_mbew" "SELECT 
                                            MATNR, 
                                            STPRS, 
                                            PEINH
                                            FROM APP_OPS.dbo.MDM_UPN_MBEW
                                            where 1=1"
											}

# ods_mdm_customermaster
sync_ods_mdm_customermaster() {
    sync_data_sqlserver "ods_mdm_customermaster" "SELECT 
                                            Custome, 
                                            Name, 
                                            Name2, 
                                            City, 
                                            PostCod, 
                                            Rg, 
                                            Searchterm, 
                                            Street, 
                                            Telephone1, 
                                            FaxNumber, 
                                            Tit, 
                                            OrBlk, 
                                            BlB, 
                                            [Group], 
                                            Cl, 
                                            Dlv, 
                                            Del, 
                                            Name3, 
                                            Name4, 
                                            Distr, 
                                            B, 
                                            TranspZone, 
                                            Cty, 
                                            DeleteFlag, 
                                            TFN, 
                                            TeleboxNumber, 
                                            PaymentBlock, 
                                            MasterRecord, 
                                            TypeofBusiness,
                                             CreatedBy, 
                                             CreatedDT, 
                                             CustomerSales, 
                                            SAPID, 
                                            DealerName, 
                                            DealerType, 
                                            ParentSAPID, 
                                            ParentDealerName, 
                                            ParentDealerType, 
                                            IsActiveInDMS
                                            FROM APP_OPS.dbo.MDM_CustomerMaster
                                            where 1=1"
											}


# 按业务分类同步数据
if [ "$1"x = "csgn"x ];then
	echo "$1 only run"
	sync_ods_trans_csgn_order
    sync_ods_trans_csgn_t2 
    sync_ods_trans_salesdealerinventory
    sync_ods_trans_consignmenttracking
    sync_ods_trans_consignmentlist
    sync_ods_kpi_complaint
    sync_ods_trans_twinventory_consignment
    sync_ods_mdm_upn_mbew
    sync_ods_mdm_customermaster
elif [ "$1"x = "t2"x ];then
    echo " $1 only run"
	sync_ods_trans_csgn_t2
elif [ "$1"x = "sales"x ];then
    echo " $1 only run"
	sync_ods_trans_salesdealerinventory
elif [ "$1"x = "tracking"x ];then
    echo " $1 only run"
	sync_ods_trans_consignmenttracking
elif [ "$1"x = "list"x ];then
    echo " $1 only run"
	sync_ods_trans_consignmentlist
elif [ "$1"x = "complaint"x ];then
    echo " $1 only run"
	sync_ods_kpi_complaint
elif [ "$1"x = "marc"x ];then
    echo " $1 only run"
	sync_ods_mdm_materialmaster_marc
elif [ "$1"x = "hk"x ];then
    echo " $1 only run"
	sync_ods_trans_twinventory_consignment
    sync_ods_mdm_upn_mbew
    sync_ods_mdm_customermaster
else
    echo "failed run"

fi    

# 设置必要的参
target_db_name='opsdw'
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#  1.业务数据SQL
#Load data into table ods_trans_openordercn from hdfs trans_openordercn by partition
ods_trans_csgn_order_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_csgn_order/$sync_date' overwrite
into table ${target_db_name}.ods_trans_csgn_order
partition(dt='$sync_date');
"
ods_mdm_materialmaster_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_mdm_materialmaster/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_materialmaster
partition(dt='$sync_date');
"
ods_trans_csgn_t2_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_csgn_t2/$sync_date' overwrite
into table ${target_db_name}.ods_trans_csgn_t2
partition(dt='$sync_date');
"

ods_trans_salesdealerinventory_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_salesdealerinventory/$sync_date' overwrite
into table ${target_db_name}.ods_trans_salesdealerinventory
partition(year_mon='$last_month_date');
"

ods_trans_consignmenttracking_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_consignmenttracking/$sync_date' overwrite
into table ${target_db_name}.ods_trans_consignmenttracking;
"

ods_trans_consignmentlist_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_consignmentlist/$sync_date' overwrite
into table ${target_db_name}.ods_trans_consignmentlist
partition(dt='$sync_date');
"

ods_kpi_complaint_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_kpi_complaint/$sync_date' overwrite
into table ${target_db_name}.ods_kpi_complaint
partition(dt='$sync_date');
"

ods_mdm_materialmaster_marc_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_mdm_materialmaster_marc/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_materialmaster_marc
partition(dt='$sync_date');
"

ods_trans_twinventory_consignment_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_trans_twinventory_consignment/$sync_date' overwrite
into table ${target_db_name}.ods_trans_twinventory_consignment
partition(dt='$sync_date');
"

ods_mdm_upn_mbew_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_mdm_upn_mbew/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_upn_mbew
partition(dt='$sync_date');
"

ods_mdm_customermaster_sql="
load data inpath '/bsc/origin_data/$origin_db_name/ods_mdm_customermaster/$sync_date' overwrite
into table ${target_db_name}.ods_mdm_customermaster
partition(dt='$sync_date');
"

# 2. 执行加载数据SQL
if [ "$1"x = "csgn"x ];then
	echo "$1 only run"
	$hive -e"$ods_trans_csgn_order_sql"
    $hive -e"$ods_trans_csgn_t2_sql"
    $hive -e"$ods_trans_salesdealerinventory_sql"
    $hive -e"$ods_trans_consignmenttracking_sql"
    $hive -e"$ods_trans_consignmentlist_sql"
    $hive -e"$ods_kpi_complaint_sql"
    $hive -e"$ods_trans_twinventory_consignment_sql"
    $hive -e"$ods_mdm_upn_mbew_sql"
    $hive -e"$ods_mdm_customermaster_sql"
elif [ "$1"x = "t2"x ];then
    echo " $1 only run"
	$hive -e"$ods_trans_csgn_t2_sql"
elif [ "$1"x = "sales"x ];then
    echo " $1 only run"
	$hive -e"$ods_trans_salesdealerinventory_sql"
elif [ "$1"x = "tracking"x ];then
    echo " $1 only run"
	$hive -e"$ods_trans_consignmenttracking_sql"
elif [ "$1"x = "list"x ];then
    echo " $1 only run"
	$hive -e"$ods_trans_consignmentlist_sql"
elif [ "$1"x = "complaint"x ];then
    echo " $1 only run"
	$hive -e"$ods_kpi_complaint_sql"
elif [ "$1"x = "marc"x ];then
    echo " $1 only run"
	$hive -e"$ods_mdm_materialmaster_marc_sql"
elif [ "$1"x = "hk"x ];then
    echo " $1 only run"
	$hive -e"$ods_trans_twinventory_consignment_sql"
    $hive -e"$ods_mdm_upn_mbew_sql"
    $hive -e"$ods_mdm_customermaster_sql"
else
    echo "failed run"

fi   


echo "End loading data on {$sync_date} ..hdfs to ods_trans_openordercn........................................................"
