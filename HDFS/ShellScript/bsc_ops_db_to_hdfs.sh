#!/bin/bash
# Function:
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
	year_month=$(date  +'%Y-%m')
	yestermon=$(date -d '-30 day' +%F)
    yesterthreeday=$(date -d '-3 day' +%F)
    yesterday=$(date -d '-1 day' +%F)
fi

# 设置同步起始日期
start_date=$(date -d "-1day" "+%F")
echo "period sync up from ${start_date}....."

#同步MySQL数据通过sqoop
sync_data_mysql() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect jdbc:mysql://172.25.48.1:3306/$sync_db \
        --username root \
        --password 1qazXSW@ \
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

#同步SQl Server数据通过sqoop 按空格分隔
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

#同步SQl Server数据通过sqoop 按逗号分隔
sync_data_to_sqlserver() {
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

# 同步国外发货到国内收货数据
# 同步策略 - 取updatedt等于当天的
sync_trans_ctmshipmentstatus_data() {
    echo "Start syncing trans_ctmshipmentstatus data information"
    sync_data_to_sqlserver "trans_ctmshipmentstatus" "SELECT 
														ID
														,UpdateDT
														,Active
														,WorkNumber
														,CommercialInvoice
														,BSCInformSLCDate
														,T1PickUpDate
														,ActualArrivalTime
														,DockWarrantDate
														,ForwordingInformSLCPick
														,Forwording
														,IntoInventoryDate
														,UpdateDate
														,ShipmentInternalNumber
														,MasterBillNo
														,HouseWaybillNo
														,ImportExportFlag
														,ShipmentType
														,EmergencySigns
														,Merchandiser
														,VoucherMaker
														,AbnormalCauses1
														,AbnormalCauses2
														,InspectionMark1
														,InspectionMark2
														,InspectionMark3
														,Remark
														,Quantity
														,GrossWeight
														,ForwarderServiceLevel
														,Department
														,CountryArea
														,TransportationType
														,CustomsSupervisionCertificate
														,CommodityInspectionDemand
														,CustomizedCertificate
														,ETD
														,ETA
														,ReviseETD
														,ReviseETA
														,CommodityInspection
														,CustomsInspection
														,DeclarationCompletionDate
														,CommericalInvoce
														,DraftCompletionDate
														,EDIReleaseDate
														,TaxPaymentCompletionDate
														,CustomsClaranceComplete
														,InspectionQuarantineCertificateCompleteDate
														,PickupdocumentsRecievedDate
														,PreInspectionStartDate
														,CustomsClearanceOrderReceived
														,InspectionSlipReceived
														,WorkContactListComplete
														,TestDate
														,CIQComplete
														,InspectionStartTime
														,DistributionListReceivedDate
														,InfoType
														FROM APP_OPS.dbo.TRANS_CTMShipmentStatus
														--where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
                                                        --and DockWarrantDate is not null
														where 1=1"

    echo "End syncing trans_ctmshipmentstatus master data information"

}

# 同步Plant主数据
# 同步策略 - 全量
sync_plant_master_data() {
    echo "Start syncing plant master data information"
    sync_data_sqlserver "plant" "select PlantCode,
                                        SearchTerm2,
                                        SearchTerm1,
                                        PostlCode,
                                        City,
                                        Name2,
                                        Name1
                                        from MDM_Plant
                                        where 1=1"

    echo "End syncing plant master data information"

}
# 同步DealerCode主数据,同步策略 - 全量
MDM_DealerMaster_data() {
    echo "Start syncing MDM_DealerMaster master data information"
    sync_data_sqlserver "MDM_DealerMaster" "select  
											DealerCode, 
											DealerName, 
											DealerType, 
											ParentSAPID,
											ParentDealerName,
											ParentDealerType, 
											Status,
											HospitalCode,
											DealerAddress,
											IsActiveInDMS,
											DealerMarketType,
											DealerMarketTypeDesc,
											DealerNameEn,
											DealerAddrProvince,
											DealerAddrCity, 
											DealerTypeAlt
                                        from MDM_DealerMaster
                                        where 1=1"
    echo "End syncing MDM_DealerMaster master data information"
}

# 同步客户主数据
# 同步策略 - 全量
sync_customer_master_data() {
    echo "Start syncing customer master data information"
    sync_data_sqlserver "customer" "select Custome
                                            ,Name
                                            ,Name2
                                            ,City
                                            ,PostCod
                                            ,Rg
                                            ,Searchterm
                                            ,Street
                                            ,Telephone1
                                            ,FaxNumber
                                            ,Tit
                                            ,OrBlk
                                            ,BlB
                                            ,[Group]
                                            ,Cl
                                            ,Dlv
                                            ,Del
                                            ,Name3
                                            ,Name4
                                            ,Distr
                                            ,B
                                            ,TranspZone
                                            ,Cty
                                            ,DeleteFlag
                                            ,TFN
                                            ,TeleboxNumber
                                            ,PaymentBlock
                                            ,MasterRecord
                                            ,TypeofBusiness
                                            ,CreatedBy
                                            ,CreatedDT
                                            ,CustomerSales
											,SAPID
											,DealerName
											,DealerType
											,ParentSAPID
											,ParentDealerName
											,ParentDealerType
											,IsActiveInDMS
                                        from MDM_CustomerMaster
                                        where 1=1"

    echo "End syncing customer master data information"

}

# 同步Division主数据,同步策略 - 全量
sync_division_master_data() {
    echo "Start syncing division master data information"
    sync_data_sqlserver "division" "select  DivisionID
                                            ,Division
                                            ,ShortName
                                            ,NameCN
                                            ,DisplayName
                                        from MDM_DivisionMaster
                                        where 1=1"
    echo "End syncing division master data information"
}

# ExchangeRate,同步策略 - 全量
sync_exchange_rate_master_data() {
    echo "Start syncing ExchangeRate master data information"
    sync_data_sqlserver "exchange_rate" "SELECT  [FROM]
                                            ,[To]
                                            ,ValidFrom
                                            ,Rate
                                            ,RatioFrom
                                            ,RatioTo
                                        FROM MDM_ExchangeRate
                                        where 1=1"
    echo "End syncing ExchangeRate master data information"
}

# 同步Location主数据,同步策略 - 全量
sync_location_master_data() {
    echo "Start syncing location master data information"
    sync_data_sqlserver "location" "select DPlant,
                                            PlantName,
                                            Location,
                                            Status,
                                            StorageLocation,
                                            StorageDefinition
                                            from MDM_StorageLocation
                                            where 1=1"
    echo "End syncing locaiton master data information"
}

# 同步批号主数据,同步策略 - 全量
sync_batch_master_data() {
    sync_data_sqlserver 'batch' "select Material, 
                                    Batch, 
                                    ShelfLifeExpDate, 
                                    CountryOfOrigin, 
                                    DateOfManuf, 
                                    CFDA
                                    from MDM_BatchMaster
                                    where 1=1"
}

# 同步IDD主数据,同步策略 -  全量
sync_idd_master_data() {
    sync_data_sqlserver 'IDD' "SELECT 
                                IDDDelivery, 
                                Material, 
                                Batch, 
                                DeclareStatus, 
                                IDDDate, 
                                IDDType, 
                                IDDQuantity, 
                                PackingListDate, 
                                SLCDate, 
                                IDDSubmitDate, 
                                T1Date, 
                                DataSupplementDelivery, 
                                IDDStatus, 
                                ShelfDate, 
                                Remark, 
                                ReceivingPlant
                                FROM MDM_IDD
                                where 1=1"
}

# 同步物料主数据，同步策略：全量
sync_material_master_data() {
    sync_data_sqlserver 'material' "select ChineseName, 
                                        Material, 
                                        OldCode, 
                                        EnglishName, 
                                        ProfitCenter, 
                                        GTIN, 
                                        SAPUPLLevel1Code, 
                                        SAPUPLLevel1Name, 
                                        SAPUPLLevel2Code, 
                                        SAPUPLLevel2Name, 
                                        SAPUPLLevel3Code, 
                                        SAPUPLLevel3Name, 
                                        SAPUPLLevel4Code, 
                                        SAPUPLLevel4Name, 
                                        SAPUPLLevel5Code, 
                                        SAPUPLLevel5Name, 
                                        SpecialProcurement, 
                                        LatestCFDA, Sheets, 
                                        MMPP, Dchain, SPK, 
                                        DefaultLocation, 
                                        SourceListIndicator, 
                                        PDT, GRT, QIFlag, LoadingGroup, 
                                        legalStatus, DeliveryPlant, 
                                        ShelfLifeSAP, StandardCost, 
                                        TransferPrice, PRA, MaterialType, 
                                        PRAValidFrom, PRAValidTo, PRAStatus, 
                                        PRAReleaseStatusCode, StandardCostUSD, 
                                        ABCClass, Division, ProductLine1Code, 
                                        ProductLine1Name, ProductLine2Code, 
                                        ProductLine2Name, ProductLine3Code, 
                                        ProductLine3Name, ProductLine4Code, 
                                        ProductLine4Name, ProductLine5Code, 
                                        ProductLine5Name, MShelfLifeForOboselete, 
                                        XYZ, MISVBP, ProductModel,
                                        DlrQuotaProductLineID,
                                        DlrQuotaProductLine,
                                        DlrAuthProductLineID,
                                        DlrAuthProductLine,
                                        SalesStatus,
                                        IsActiveForSale,
                                        SubBUCode,
                                        SubBUName,
                                        LPSubBUCode,
                                        LPSubBUName
                                        FROM MDM_MaterialMaster
                                        where 1=1"
}

# 同步日历主数据,同步策略：全量
sync_calendar_master_data() {
    sync_data_sqlserver 'calendar' "select [Date], 
                                    [Month], [Year], 
                                    Quarter, WeeknumM1,
                                    WeeknumY1, WeeknumM2, 
                                    WeeknumY2, Weekday, 
                                    Workday, WorkdayFlag, 
                                    POPattern, YearMonthDate, 
                                    MonthWeeknum, DayOfWeek
                                    from MDM_Calendar
                                    where 1=1"
}


# 同步进出口转仓单,增量
sync_import_export_sto() {
    sync_data_sqlserver 'import_export_sto' "SELECT  ID 
                                            ,UpdateDT 
                                            ,Active 
                                            ,STONo 
                                            ,STOCreateDT 
                                            ,STOUpdateDT 
                                            ,STOCreatedBy 
                                            ,STOUpdatedBy 
                                            ,STOStatus 
                                            ,STOType 
                                            ,STOOrderReason 
                                            ,OrderRemarks 
                                            ,ShipFromPlant 
                                            ,ShipToPlant 
                                            ,STOLineNo 
                                            ,Material 
                                            ,Qty 
                                            ,Unit
                                        FROM TRANS_ImportExportTransaction
                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步进出口发货单,增量 改为美国时间放到ods到dwd改
sync_import_export_sto_dn() {
    sync_data_sqlserver "import_export_sto_dn" "SELECT  ID
                                            ,UpdateDT
                                            ,Active
                                            ,STONo
                                            ,SAPDeliveryNoInbound
                                            ,SAPDeliveryNoOutbound
                                            ,DNCreateDT
                                            --,DATEADD(hh,12,DNCreateDT) as chinese_DNCreateDT
                                            ,DNStatus
                                            ,DNUpdateDT
                                            ,DNCreateBy
                                            ,DNUpdatedBy
                                            ,ReceiverCustomerCode
                                            ,SAPDeliveryLineNo
                                            ,Material
                                            ,Qty
                                            ,Batch
                                            ,ShipFromPlant
                                            ,ShipFromLocation
                                            ,ShipToPlant
                                            ,ShipToLocation
                                            ,DeliveryMode
                                            ,ActualMigoDT
                                            ,PGIDate
                                        FROM TRANS_ImportExportDelivery
                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
                                        
}

# 同步发货单对应关系,增量
#  where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
sync_inbound_outbound_dn_mapping() {
    sync_data_sqlserver "inbound_outbound_dn_mapping" "SELECT 
                                                            distinct 
                                                             '' as ID
                                                            ,UpdateDT
                                                            ,Active
                                                            ,InboundDN
                                                            ,OutboundDN
                                                        FROM TRANS_DeliveryLikp
                                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步商业发票与发货单对应关系, 增量
sync_commercial_invoice_dn_mapping() {
    sync_data_sqlserver "commercial_invoice_dn_mapping" "SELECT  ID
                                                            ,UpdateDT
                                                            ,Active
                                                            ,Delivery
                                                            ,Invoice
                                                            ,Qty
                                                            ,DATEADD(hh,12,MailReceived) as MailReceived
                                                            ,DATEADD(hh,12,SAPMIGODATE) as SAPMIGODATE
                                                        FROM TRANS_T1Invoice
                                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步本地化
sync_work_order() {
    sync_data_sqlserver "work_order" "SELECT
                                        UpdateDT
                                        ,Plant
                                        ,CommercialInvoiceNo
                                        ,SAPDeliveryNo
                                        ,WorkorderNo
                                        ,WorkorderCreateDT
                                        ,WorkorderCreateBy
                                        ,WorkorderStatus
                                        ,WorkorderStartDT
                                        ,WorkorderStartedBy
                                        ,WorkorderCompleteDT
                                        ,WorkorderReleaseDT
                                        ,WorkorderReleaseBy
                                        ,WorkorderLineNo
                                        ,Material
                                        ,Batch
                                        ,CurrentQty
                                        ,ProcessedQty
                                        ,QRCode
                                        ,ReleaseQty
                                    FROM TRANS_Workorder
                                    where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步国内转仓订单
sync_domestic_sto() {
    sync_data_sqlserver "domestic_sto" "SELECT  ID
                                            ,UpdateDT
                                            ,Active
                                            ,STONo
                                            ,STOCeateDT
                                            ,STOCreateBy
                                            ,STOUpdateDT
                                            ,STOUpdatedBy
                                            ,STOStatus
                                            ,STORemarks
                                            ,STOType
                                            ,STOReason
                                            ,ShipFromPlant
                                            ,ShipToPlant
                                            ,STOLineNo
                                            ,Material
                                            ,QTY
                                            ,Unit
                                        FROM TRANS_DomesticTransaction
--                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步国内转仓发货单
sync_domestic_sto_dn() {
    sync_data_sqlserver "domestic_sto_dn" "SELECT  ID
                                            ,UpdateDT
                                            ,Active
                                            ,STONo
                                            ,SAPDeliveryNoInbound
                                            ,SAPDeliveryNoOutbound
                                            ,DNCreateDT                                            
                                            ,DNCreateBy
                                            ,DNUpdateDT
                                            ,DNUpdateBy
                                            ,DNStatus
                                            ,SAPDeliveryLineNo
                                            ,Material
                                            ,Qty
                                            ,Batch
                                            ,ShipFromPlant
                                            ,ShipFromLocation
                                            ,ShipToPlant
                                            ,ShipToLocation
                                            ,QRCode
                                            ,DeliveryMode
                                            ,carrier
                                            ,DATEADD(hh,12,ActualMigoDT) as ActualMigoDT
                                            ,DATEADD(hh,12,PGIDate) as PGIDate
                                            ,DATEADD(hh,12,DNCreateDT) as chinese_DNCreateDT
                                        FROM TRANS_DomesticDelivery
                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步销售订单
sync_sales_order() {
    sync_data_sqlserver "sales_order" "SELECT  ID
                                            ,UpdateDT
                                            ,Active
                                            ,SONo
                                            ,OrderType
                                            ,OrderReason
                                            ,RejectReason
                                            ,OrderRemarks
                                            ,SOCreateDT                                            
                                            ,SOUpdateDT
                                            ,SOCreateBy
                                            ,SOUpdatedBy
                                            ,SOStatus
                                            ,PoNumber
                                            ,SalesOrg
                                            ,StorageLoc
                                            ,SOLineNo
                                            ,Material
                                            ,Batch
                                            ,ProfitCenter
                                            ,DeliveryDate
                                            ,QTY
                                            ,NetValue
                                            ,Currency
                                            ,DeliveryBlock
                                            ,BillingBlock
                                            ,Unit
                                            ,RequestDeliveryDate
                                            ,PickUpPlant
                                            ,CustomerCode
                                            ,ShipToCode
                                            ,DATEADD(hh,12,SOCreateDT) as chinese_SOCreateDT
                                        FROM TRANS_SalesOrder
                                       where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步销售发货单
sync_sales_order_dn() {
    sync_data_sqlserver "sales_order_dn" "SELECT  ID
                                                ,UpdateDT
                                                ,Active
                                                ,SONo
                                                ,SAPDeliveryNo
                                                ,DNCreateDT                                                
                                                ,DNUpdateDT
                                                ,DNCreateBy
                                                ,DNUpdatedBy
                                                ,ShipTo
                                                ,RealShipToAddress
                                                ,DeliveryLine
                                                ,Material
                                                ,QTY
                                                ,QRCode
                                                ,Batch
                                                ,PlannedGIDate
                                                ,ActualGIDate  ---本来就是中国时间
                                                ,DeliveryMode
                                                ,Carrier
                                                ,PickLocation
						                        ,Plant
                                                ,DATEADD(hh,12,DNCreateDT) as chinese_DNCreateDT
                                            FROM TRANS_SalesDelivery
                                            where format(DNCreateDT, 'yyyy-MM-dd')>=CONVERT(varchar(7),DATEADD(dd,-90,'$sync_date'),120)+'-01'"
}
# 同步三分成品采购订单
sync_purchase_order() {
    sync_data_sqlserver "purchase_order" "SELECT  ID
                            ,UpdateDT
                            ,Active
                            ,PurchaseOrder
                            ,POCreateDT
                            ,POCreateBy
                            ,POUpdatedDT
                            ,POUpdatedBy
                            ,POStatus
                            ,POLineNo
                            ,Material
                            ,Qty
                            ,Unit
                            ,PurchasePrice
                            ,Currency
                            ,MIGODate
                            ,Batch
                            ,ReceivedQty
                        FROM TRANS_ThirdPartyPurchaseOrder
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}
# 同步进出口Inbound tracking
sync_inbound_tracking() {
    sync_data_sqlserver "inbound_tracking" "SELECT ID
                            ,UpdateDT
                            ,Active
                            ,WorkNumber
                            ,CommercialInvoice
                            ,BSCInformSLCDate
                            ,T1PickUpDate
                            ,ActualArrivalTime
                            ,DockWarrantDate
                            ,ForwordingInformSLCPick
                            ,Forwording
                            ,IntoInventoryDate
                            ,UpdateDate
                            ,ShipmentInternalNumber
                            ,MasterBillNo
                            ,HouseWaybillNo
                            ,ImportExportFlag
                            ,ShipmentType
                            ,EmergencySigns
                            ,Merchandiser
                            ,VoucherMaker
                            ,AbnormalCauses1
                            ,AbnormalCauses2
                            ,InspectionMark1
                            ,InspectionMark2
                            ,InspectionMark3
                            ,Remark
                            ,Quantity
                            ,GrossWeight
                            ,ForwarderServiceLevel
                            ,Department
                            ,CountryArea
                            ,TransportationType
                            ,CustomsSupervisionCertificate
                            ,CommodityInspectionDemand
                            ,CustomizedCertificate
                            ,ETD
                            ,ETA
                            ,ReviseETD
                            ,ReviseETA
                            ,CommodityInspection
                            ,CustomsInspection
                            ,DeclarationCompletionDate
                        FROM TRANS_CTMShipmentStatus
                        where format(UpdateDate, 'yyyy-MM-dd')>='$yestermon'"
}

# 同步Sales invoice
sync_so_invoice_info() {
    sync_data_sqlserver "so_invoice" "SELECT 
                             BillDoc
                            ,Item
                            ,AccountingNo
                            ,BillDate
                            ,BillType
                            ,SalesDoc
                            ,OrderReason
                            ,ItemCategory
                            ,PurchaseOrder
                            ,Material
                            ,ProfitCenter
                            ,Batch
                            ,BillQuantity
                            ,Net
                            ,Currency
                            ,Payer
                            ,SoldToPt
                            ,CustomerName
                            ,Classification
                            ,City
                            ,SalesRepId
                            ,Name3
                            ,Desc1
                            ,Desc2
                            ,Sale
                            ,ManufactoryDate
                            ,ExpDate
                            ,SalesLine
                            ,Delivery
                            ,ShipTo
                            ,StockLocationPt
                            ,StockLocationNM
                            ,TaxAmount
                            ,TaxRate
                            ,SalesType
                             , format(DATEADD(hh,12,CreatedDT), 'yyyy-MM-dd HH:mm:ss') as chinese_BillDate
                        FROM TRANS_Invoice
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步 WMS putaway 信息
sync_putaway_info() {
    sync_data_sqlserver "putaway" "SELECT 
                             UpdateDT
                            ,Invoice
                            ,DeliveryNo
                            ,PutAwayDate
                            ,UPN
                            ,QTY
                            ,Batch
                            ,Plant
                            ,SL
                            ,Unit
                            ,FromSAPLocation
                            ,SAPLocation
                        FROM TRANS_PutAway
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步库存交易记录
sync_inventory_movement() {
    sync_data_sqlserver "movement" "SELECT DISTINCT
                            cast(DATEADD(hh,12,cast((CONVERT(varchar(30), EnterDate) + ' ' + CONVERT(varchar(8), [Time])) AS datetime)) as date) AS  UpdateDT
                            ,MvtType
                            ,ReasonCode
                            ,SpecialStock
                            ,MaterialDoc
                            ,MatItem
                            ,StockLoc
                            ,Plant
                            ,Material
                            ,Batch
                            ,Quantity
                            ,SLEDBBD
                            ,PostingDate
                            ,cast(DATEADD(hh,12,cast((CONVERT(varchar(30), EnterDate) + ' ' + CONVERT(varchar(8), [Time])) AS datetime)) as time) as [Time]
                            ,UserName
                            ,Delivery
                            ,SAPPONumber
                            ,PoItem
                            ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (HeaderText , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as HeaderText
                            ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (OriginalReference , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as OriginalReference
                            , EnterDate
                        FROM TRANS_InventoryTransactions
                        -- where format(UpdateDT, 'yyyy-MM-dd')>='2022-03-15'
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
                        
}

# 同步日库存记录
sync_inventory_onhand() {
    sync_data_sqlserver "inventory_onhand" "SELECT 
                            UpdateDT
                            ,Active
                            ,[Date]
                            ,InventoryType
                            ,PlantFrom
                            ,PlantTo
                            ,StorageLoc
                            ,PDT
                            ,PGIDate
                            ,Delivery
                            ,DeliveryLine
                            ,MarkedInHouse
                            ,TransportOrder
                            ,ProficCenter
                            ,Material
                            ,Batch
                            ,Quantity
                            ,Unrestricted
                            ,Inspection
                            ,BlockedMaterial
                            ,ExpirationDate
                            ,StandardCost
                            ,ExtendedCost
                            ,QNInfo
                            ,UpdateDate
                            ,EOMYM
                            ,BUFlag
                            ,URQtyFlag
                            ,QIQtyFlag
                            ,BLKQtyFlag
                            ,ExpirationDateFlag
                            ,ShortDatedShelfFlag
                            ,InboundDelivery
                        FROM TRANS_InventoryOnhand
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

sync_so_receiving_confirmation_date() {
    sync_data_sqlserver "so_receiving_confirmation" "select  
                                            UpdateDT
                                            ,Delivery
                                            ,MinConfirmationDT
                                            ,MaxConfirmationDT
                                        from TRANS_ReceivingConfirmation
                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

sync_dealer_quotation() {
    sync_data_sqlserver "dealer_purchase_quotation" "SELECT
                            UpdateDT
                            ,[Year]
                            ,Division
                            ,SubBUName
                            ,SAPID
                            ,DealerName
                            ,ParentSAPID
                            ,ParentDealerName
                            ,DealerType
                            ,RSM
                            ,ZSM
                            ,TSM
                            ,ContractStartDate
                            ,ContractEndDate
                            ,MarketType
                            ,ContractStatus
                            ,NewOldDealerByBU
                            ,NewOldDealerByBSC
                            ,AOPType
                            ,Month1Amount
                            ,Month2Amount
                            ,Month3Amount
                            ,Q1Amount
                            ,Month4Amount
                            ,Month5Amount
                            ,Month6Amount
                            ,Q2Amount
                            ,Month7Amount
                            ,Month8Amount
                            ,Month9Amount
                            ,Q3Amount
                            ,Month10Amount
                            ,Month11Amount
                            ,Month12Amount
                            ,Q4Amount
                            ,YearTotalAmount
                            ,BICode
                            ,BIName
                        FROM TRANS_QuotaDealerPurchase
                        --where format(UpdateDT, 'yyyy-MM-dd')='$yesterday'
                        where 1=1"
}

# 同步Customer_KNB1主数据,同步策略 -  全量
sync_knb1_master_data() {
    sync_data_sqlserver "knb1" "SELECT 
                                CustomerNumber, 
                                AccountNumber,
                                CompanyCode
                                FROM MDM_CustomerMaster_KNB1 
                                where 1=1"
}

# 同步Customer_KNVI主数据,同步策略 -  全量
sync_knvi_master_data() {
    sync_data_sqlserver "knvi" "SELECT 
                                CustomerNumber, 
                                TaxCategory,
                                TaxClassification
                                FROM MDM_CustomerMaster_KNVI 
                                where 1=1"
}

# 同步Customer_KNVV主数据,同步策略 -  全量
sync_knvv_master_data() {
    sync_data_sqlserver "knvv" "SELECT 
                                CustomerNumber, 
                                Currency,
                                DeliveringPlant,
                                ShippingConditions,
                                SalesOrganization,
                                DeliveryPriority,
                                OrderCombinationIndicator,
                                IncotermsPart1,
                                IncotermsPart2
                                FROM MDM_CustomerMaster_KNVV 
                                where 1=1"
}

# 同步so_createinfo销售订单创建数据,同步策略 -  增量
sync_so_createinfo_data() {
    sync_data_sqlserver "so_createinfo" "SELECT 
                                               SONo, 
                                               RequestDeliveryDate,
                                               SOCreateDT,
                                               SOCreateBy
                                          FROM TRANS_SalesOrder_CreatedInfo
                                         WHERE format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步so_text销售订单备注,同步策略 -  增量
sync_so_text_data() {
    sync_data_sqlserver "so_text" "SELECT 
                                               SONo, 
                                               PickList
                                          FROM TRANS_SalesOrder_Text
                                         WHERE format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步so_text销售订单备注,同步策略 -  增量
sync_so_partner_data() {
    sync_data_sqlserver "so_partner" "SELECT 
                                               SONo, 
                                               CustomerFunction,
                                               Customer,
                                               Carrier
                                          FROM TRANS_SalesOrder_Partner
                                         WHERE format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步workorder与qrcode的对应关系,同步策略 -  增量
sync_woqrcode_data() {
    sync_data_postgresql "woqrcode" "SELECT 
                                          plant_id, 
                                          wo_order_no,
                                          dn_no,
                                          material,
                                          batch,
                                          qrcode
                                     FROM wo_qrcode
                                    WHERE 1=1"
}

# 同步注册证与产品对应关系,同步策略 -  全量
sync_cfda_data() {
    sync_data_sqlserver "cfda" "SELECT 
                                          RegistrationNO,
                                          DepCode,
                                          UPNName,
                                          ProductNameEn,
                                          CommerceName,
                                          GMKind,
                                          GMCatalog,
                                          NewGMCatalog,
                                          Manufacturer,
                                          ManufacturerAddress,
                                          AddrOfManufacturingSite,
                                          RegisterInfo,
                                          RegisterAddress,
                                          SterilizationMode,
                                          SterilizationValidity,
                                          ServiceYear,
                                          ProductMaterial,
                                          StorageConditions,
                                          StorageConditionsTemperature,
                                          TransportCondition,
                                          TransportConfitionTemperature,
                                          ValidFrom,
                                          ValidTo,
                                          IsActived,
                                          CreatedDate,
                                          LastModifiedDate
                                     FROM MDM_CFDA
                                    WHERE 1=1"
}

# 同步注册证信息,同步策略 -  全量
sync_cfda_upn_data() {
    sync_data_sqlserver "cfda_upn" "SELECT 
                                          RegistrationNo, 
                                          UPN,
                                          ValidDateFrom,
                                          ValidDateTo
                                     FROM MDM_CFDA_UPN
                                    WHERE 1=1"
}

#同步ctm——customermaster -  全量
sync_CTMCustomerMaster() {
    sync_data_sqlserver "CTMCustomerMaster" "select
                                                  Site,
                                                  Material,
                                                  ProductType,
                                                  ChineseName,
                                                  Model,
                                                  EnglishName,
                                                  HSCode,
                                                  HSAdditionalCode,
                                                  EnterpriseUnit,
                                                  DeclareUnit,
                                                  DeclarationScaleFactor,
                                                  Currency,
                                                  UnitPrice,
                                                  OriginCountry,
                                                  FirstLegalUnit,
                                                  FirstScaleFactor,
                                                  SecondLegalUnit,
                                                  SecondScaleFactor,
                                                  StartExpiryDate,
                                                  BondedProperty,
                                                  MaterialsFlag,
                                                  SpecialRemark,
                                                  Remark,
                                                  PreClassificationNumber,
                                                  UpdateReason,
                                                  Quantity,
                                                  NetWeight,
                                                  GrossWeight,
                                                  Length,
                                                  Width,
                                                  Height,
                                                  MrpController,
                                                  ManualModel,
                                                  MaterialGroup,
                                                  CustomsClassificationQA,
                                                  DistributionProperties,
                                                  ImportDocumentRequirements,
                                                  ExportDocumentRequirements,
                                                  EndExpiryDate,
                                                  SupervisionCertificate,
                                                  InspectionRequirements,
                                                  MFNTaxRate,
                                                  ProvisionalTaxRate,
                                                  CreationDate,
                                                  CreationBy,
                                                  LastModifyDate,
                                                  LastModifyBy
                                            from MDM_CTMCustomerMaster
                                            where 1=1"
}


#同步CTMCustomsPermissionCertification -  全量
sync_CTMCustomsPermissionCertification() {
    sync_data_sqlserver "CTMCustomsPermissionCertification" "select
                                                                  Number,
                                                                  ContractNo,
                                                                  ApplicationNo,
                                                                  Category,
                                                                  CategoryDescription,
                                                                  Type,
                                                                  TypeDescription,
                                                                  MAXUsage,
                                                                  RemainingUsage,
                                                                  StartDate,
                                                                  StopDate,
                                                                  DEDTypeCode,
                                                                  DEDTypeName,
                                                                  DeductionTypeCode,
                                                                  DeductionTypeDesc,
                                                                  DomesticConsigneeCode,
                                                                  DomesticConsigneeUSCC,
                                                                  DomesticConsignor,
                                                                  OwnerCode,
                                                                  OwnerUSCC,
                                                                  OwnerName,
                                                                  TradeCountry,
                                                                  PermitOriginDestinationCountry,
                                                                  CustomsJurisdiction,
                                                                  CIQJurisdiction,
                                                                  Destination,
                                                                  Requestor,
                                                                  ResponsiblePerson,
                                                                  VeriifcationStatusCode,
                                                                  VeriifcationStatusDesc,
                                                                  VerificationDate,
                                                                  StopAlarm,
                                                                  CompanyTransportationMethod,
                                                                  SequenceNumber,
                                                                  PartNumber,
                                                                  ChineseDescription,
                                                                  HSCode,
                                                                  CanUseQty,
                                                                  UseQty,
                                                                  RemainingQty,
                                                                  CustomsUM,
                                                                  AvailableAmount,
                                                                  UsedAmount,
                                                                  RemainingAmount,
                                                                  Currency,
                                                                  RecordNetWeight,
                                                                  UsedNETWeight,
                                                                  RemainNetWeight,
                                                                  CostCenter,
                                                                  Division,
                                                                  CheckFlag,
                                                                  CheckNumber,
                                                                  CheckStatus,
                                                                  Usage,
                                                                  PartModel,
                                                                  Brand,
                                                                  OriginDestinationCountry
                                            from MDM_CTMCustomsPermissionCertification
                                            where 1=1"
}

# 同步CTMIntegrationQuery,全量
sync_CTMIntegrationQuery() {
    sync_data_sqlserver "CTMIntegrationQuery" "SELECT  UpdateDT
                                                       ,Active
                                                       ,CostCenter
                                                       ,DivisionNumber
                                                       ,ForwarderReferenceID
                                                       ,ShipmentNumber
                                                       ,BLNo 
                                                       ,ShipmentCreateDate
                                                       ,InternalCCSNo
                                                       ,CCSNo
                                                       ,HBNo
                                                       ,RegisteredItemID
                                                       ,ImportExport
                                                       ,TransportationMethod
                                                       ,TradeMode
                                                       ,TradingCountryRegion
                                                       ,Incoterms
                                                       ,CCSCreateDate
                                                       ,ImportExportDate
                                                       ,DeclarationDate
                                                       ,EntryOperator
                                                       ,TrackingOperator
                                                       ,CommercialInvoiceNo
                                                       ,CommercialInvoiceSequenceNumber
                                                       ,CustomsManifestDetailNo
                                                       ,CCSItemNumber
                                                       ,CustomsItemNo
                                                       ,PartNumber
                                                       ,HSCode
                                                       ,ChineseDescription
                                                       ,EnglishPart
                                                       ,BOMVersion
                                                       ,DescriptionBondedFlag
                                                       ,OriginCountry
                                                       ,DestinationCountryRegion
                                                       ,OriginDestinationCountry
                                                       ,DeclQty
                                                       ,CustomsUM
                                                       ,DeclaredUnitPrice
                                                       ,Currency
                                                       ,DeclarationPrice
                                                       ,TotalAmount
                                                       ,CustomsBOMVersion
                                                       ,Qty1
                                                       ,Unit1
                                                       ,DECLAREPRICE1
                                                       ,Qty2
                                                       ,Unit2
                                                       ,SenderReceiver
                                                       ,NetWeight
                                                       ,ProjectNumber
                                                       ,PONumber
                                                       ,DPP
                                                       ,CountryTax
                                                       ,ProvisionalTax
                                                       ,CustomsDutyRate
                                                       ,ActualTaxRate
                                                       ,ProtocolType
                                                       ,VATRate
                                                       ,ConsumptionTaxRate
                                                       ,EstimatedCustomsDuty
                                                       ,EstimatedVAT
                                                       ,EstimatedConsumptionTax
                                                       ,MOFCOMApprovalNumber
                                                       ,ItemGroupId
                                                       ,VoyageNumber
                                                       ,UsageType
                                                       ,UsageCode
                                                       ,ShipmentType
                                                       ,Forwarder
                                                       ,Broker
                                                       ,DeclareMode
                                                       ,BillTo
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (MBLMAWB , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') HBLHAWB
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (MBLMAWB , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as MBLMAWB
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (Planner , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as Planner
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (RegistrationNumber , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as RegistrationNumber
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (TrackingID , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as TrackingID
                                                       ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (SITE , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as SITE
                                                       ,TradingCompanyCode
                                                       ,TradingCompanyName
                                                       ,ReceiverCompanyCode
                                                       ,ReceiverCompanyName
                                                       ,FinanceInvoiceNumber
                                                       ,POSequence
                                                       ,SIDRemark
                                                       ,BrokerPOANumber
                                                       ,CommercialInvoicePrice
                                                       ,CommercialInvoiceCurrency
                                                       ,BondedManifest
                                                       ,BondedManifestDeclareDate
                                                       ,BondedManifestDuMark
                                                       ,BMCustomsDeclarationSign
                                                       ,BMCustomsDeclarationType
                                                       ,RelationCCS
                                                       ,DeclarationForm
                                                       ,ListingNumber
                                                       ,AntiDumpingDuty
                                                       ,ContractNo
                                                       ,UPN
                                        FROM TRANS_CTMIntegrationQuery                                        
                                        where format(UpdateDT, 'yyyy-MM-dd')>='$yesterthreeday'"
}


# 同步DutybyUPN,增量
#  where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
sync_DutybyUPN() {
    sync_data_sqlserver "DutybyUPN" "SELECT  UpdateDT
                                             ,Active
                                             ,CustomsDeclarationNo
                                             ,SeriaNumber
                                             ,TaxNumber
                                             ,TaxType
                                             ,ContractNo
                                             ,TaxBillCreateDT
                                             ,RecordArea
                                             ,Amount
                                             ,TaxPaymentDate
                                             ,MailDeliveryDate
                                             ,UPN
                                             ,Quantity
                                             ,BU
                                             ,YEARMONTH
                                                        FROM TRANS_DutybyUPN
                                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}


# 同步TRANS_RRSalesForecast,增量
#  where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
sync_RRSalesForecast() {
    sync_data_sqlserver "RRSalesForecast" "SELECT  UpdateDT
                                                   ,Active
                                                   ,Part
                                                   ,SuperDivision
                                                   ,Division2
                                                   ,Division
                                                   ,ProcurementSegment
                                                   ,ProductGroup
                                                   ,Family
                                                   ,Type
                                                   ,Part1
                                                   ,Month1
                                                   ,Month2
                                                   ,Month3
                                                   ,Month4
                                                   ,Month5
                                                   ,Month6
                                                   ,Month7
                                                   ,Month8
                                                   ,Month9
                                                   ,Month10
                                                   ,Month11
                                                   ,Month12
                                                   ,Month13
                                                   ,Month14
                                                   ,Month15
                                                   ,Month16
                                                   ,Month17
                                                   ,Month18
                                                   ,Month19
                                                   ,Month20
                                                   ,Month21
                                                   ,Month22
                                                   ,Month23
                                                   ,Month24
                                                   ,ForcastVersion
                                                   ,UpdateDate
                                                   ,ForcastCycle
                                                   ,PLANT
                                                        FROM TRANS_RRSalesForecast
                                                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'"
}

# 同步TRANS_FS10NDetail,全量
sync_TRANS_FS10NDetail() {
    sync_data_sqlserver "TRANS_FS10NDetail" "SELECT 
                                                ID, 
                                                UpdateDT,
                                                Acitve, 
                                                DocumentNo,
                                                PstngDate, 
                                                LCurr, 
                                                PK, 
                                                Typ,
                                                ProfitCtr,
                                                Itm, 
                                                Account,
                                                [Text],
                                                UserName, 
                                                DocDate,
                                                AmountInDocCurr,
                                                Curr,
                                                AmountInLocalCur,
                                                Material,
                                                CostCtr ,
                                                left(DocDate,7) as YearMonth
                                    FROM APP_OPS.dbo.TRANS_FS10NDetail 
                                    WHERE 1=1"
}
# 同步TRANS_FS10N,全量
sync_TRANS_FS10N() {
    sync_data_sqlserver "TRANS_FS10N" "SELECT 
                                        ID, 
                                        UpdateDT, 
                                        Active, 
                                        Period, 
                                        Debit, 
                                        Credit, 
                                        Balance, 
                                        Cumbalance,
                                        [Year]
                                    FROM APP_OPS.dbo.TRANS_FS10N
                                    WHERE 1=1"
}
# 同步 TRANS_WorkOrder_OrdZ,全量
sync_TRANS_WorkOrder_OrdZ() {
    sync_data_to_sqlserver "TRANS_WorkOrder_OrdZ" "SELECT 
                                        ID, 
                                        UpdateDT,
                                        Active, 
                                        Plant, 
                                        SAPDeliveryNo,
                                        WorkorderNo, 
                                        WorkorderCreateDT,
                                        WorkorderStartDT, 
                                        WorkorderENDDT,
                                        WorkorderReleaseDT,
                                        Shipment,
                                        MaterialDoc,
                                        HeaderText,
                                        Material, 
                                        Batch, 
                                        OriginalQty,
                                        CurrentQty, 
                                        ReleasedQty,
                                        SalesDoc
                                        FROM APP_OPS.dbo.TRANS_WorkOrder_OrdZ 
                                        WHERE 1=1                                   
                                        ---WHERE format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
                                        "
}
# 按业务分类同步数据
case $1 in
"plant")
    sync_plant_master_data
    ;;
"TRANS_FS10NDetail")
    sync_TRANS_FS10NDetail
    ;;
"TRANS_WorkOrder_OrdZ")
    sync_TRANS_WorkOrder_OrdZ
    ;;
"TRANS_FS10N")
    sync_TRANS_FS10N
    ;;
"trans_ctmshipmentstatus")
    sync_trans_ctmshipmentstatus_data
    ;;
"MDM_DealerMaster")
    MDM_DealerMaster_data
    ;;	
"location")
    sync_location_master_data
    ;;
"batch")
    sync_batch_master_data
    ;;
"IDD")
    sync_idd_master_data
    ;;
"material")
    sync_material_master_data
    ;;
"calendar")
    sync_calendar_master_data
    ;;
"import_export_sto")
    sync_import_export_sto
    ;;
"import_export_sto_dn")
    sync_import_export_sto_dn
    ;;
"in_out_map")
    sync_inbound_outbound_dn_mapping
    ;;
"invoice_dn_map")
    sync_commercial_invoice_dn_mapping
    ;;
"wo")
    sync_work_order
    ;;
"domestic_sto")
    sync_domestic_sto
    ;;
"domestic_sto_dn")
    sync_domestic_sto_dn
    ;;
"so")
    sync_sales_order
    ;;
"so_dn")
    sync_sales_order_dn
    ;;
"so_invoice")
    sync_so_invoice_info
    ;;
"putaway")
    sync_putaway_info
    ;;
"movement")
    sync_inventory_movement
    ;;
"onhand")
    sync_inventory_onhand
    ;;
"po")
    sync_purchase_order
    ;;
"inbound_tracking")
    sync_inbound_tracking
    ;;
"cust")
    sync_customer_master_data
    ;;
"division")
    sync_division_master_data
    ;;
"exchange_rate")
    sync_exchange_rate_master_data
    ;;
"socd")
    sync_so_receiving_confirmation_date
    ;;
"dpq")
    sync_dealer_quotation
    ;;
"knb1")
    sync_knb1_master_data
    ;;
"knvi")
    sync_knvi_master_data
    ;;
"knvv")
    sync_knvv_master_data
    ;;
"so_createinfo")
    sync_so_createinfo_data
    ;;
"so_text")
    sync_so_text_data
    ;;
"so_partner")
    sync_so_partner_data
    ;;
"RRSalesForecast")
    sync_RRSalesForecast
    ;;
"DutybyUPN")
    sync_DutybyUPN
    ;;
"CTMIntegrationQuery")
    sync_CTMIntegrationQuery
    ;;
"CTMCustomsPermissionCertification")
    sync_CTMCustomsPermissionCertification
    ;;
"CTMCustomerMaster")
    sync_CTMCustomerMaster
    ;;
"master_data")
    sync_plant_master_data
    sync_location_master_data
    sync_material_master_data
    sync_calendar_master_data
    sync_idd_master_data
    sync_customer_master_data
    sync_division_master_data
    sync_exchange_rate_master_data
    sync_batch_master_data
    sync_knb1_master_data
    sync_knvi_master_data
    sync_knvv_master_data
    sync_cfda_data
    sync_cfda_upn_data
    ;;
"trans")
    sync_import_export_sto
    sync_import_export_sto_dn
    sync_inbound_outbound_dn_mapping
    sync_commercial_invoice_dn_mapping
    sync_domestic_sto
    sync_domestic_sto_dn
    sync_purchase_order
    sync_work_order
    sync_inbound_tracking
    sync_sales_order
    sync_sales_order_dn
    sync_so_invoice_info
    sync_putaway_info
    sync_inventory_movement
    sync_inventory_onhand
    sync_so_receiving_confirmation_date
    sync_dealer_quotation
    sync_so_createinfo_data
    sync_so_text_data
    sync_so_partner_data
    sync_woqrcode_data
    sync_CTMIntegrationQuery
    sync_DutybyUPN
    sync_RRSalesForecast
    sync_CTMCustomsPermissionCertification
    sync_CTMCustomerMaster
	sync_trans_ctmshipmentstatus_data
    ;;
"all")
    sync_plant_master_data
    sync_location_master_data
    sync_material_master_data
    sync_calendar_master_data
    sync_idd_master_data
    sync_customer_master_data
    sync_division_master_data
    sync_exchange_rate_master_data
    sync_batch_master_data
    sync_import_export_sto
    sync_import_export_sto_dn
    sync_inbound_outbound_dn_mapping
    sync_commercial_invoice_dn_mapping
    sync_domestic_sto
    sync_domestic_sto_dn
    sync_purchase_order
    sync_work_order
    sync_inbound_tracking
    sync_sales_order
    sync_sales_order_dn
    sync_so_invoice_info
    sync_putaway_info
    sync_inventory_movement
    sync_inventory_onhand
    sync_so_receiving_confirmation_date
    sync_dealer_quotation
    sync_knb1_master_data
    sync_knvi_master_data
    sync_knvv_master_data
    sync_so_createinfo_data
    sync_so_text_data
    sync_so_partner_data
    sync_woqrcode_data
    sync_cfda_data
    sync_cfda_upn_data
    sync_CTMCustomerMaster
    sync_CTMCustomsPermissionCertification
    sync_CTMIntegrationQuery
    sync_DutybyUPN
    sync_RRSalesForecast
	MDM_DealerMaster_data
	sync_trans_ctmshipmentstatus_data
    ;;
*)
    echo "plesase use master_data, trans, all!"
    ;;
esac
