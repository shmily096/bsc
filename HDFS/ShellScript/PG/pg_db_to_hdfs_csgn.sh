#!/bin/bash
# Function:
#   sync up PG data to HDFS template
# History:
#   2021-11-08    Donny   v1.0    init

# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
# if [ -n "$2" ] ;then 
#     sync_db=$2
# else
#    echo 'please input 第二个变量 the PostgreSQL db to be synced!'
#    exit 1
# fi

sync_db='tableaudb'

# 3 设置数据库连接字符串
connect_str_pg="jdbc:postgresql://10.226.98.58:55433/$sync_db"

# 4 同步日期设置，默认同步当天数据
if [ -n "$3" ]; then
    sync_date=$3
else
    sync_date=$(date  +%F)
fi

#5 DB User&Password -- TO be udpated
user='bsc1'
pwd='bsc1qazxsw2'

#同步PostgreSQL数据通过sqoop
sync_data_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
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

# 同步DEMO数据
# 同步策略 - 全量
sync_demo_data() {
    echo "Start syncing demo data information"
    sync_data_pg "demo" "select 
                            id, 
                            name, 
                            age 
                            from demo 
                            where 1=1"

    echo "End syncing plant master data information"
}


# 同步ods_csgn_stock数据
# 同步策略 - 全量
sync_ods_csgn_stock_data() {
    echo "Start syncing ods_csgn_stock data information"
    sync_data_pg "ods_csgn_stock" "SELECT 
                         divisionid, 
                         division, 
                         customersite, 
                         customernumber, 
                         customername, 
                         material, 
                         materialdescription, 
                         productlineid2, 
                         productline2, 
                         productlineid3, 
                         productline3, 
                         productlineid4, 
                         productline4, 
                         batch, 
                         expirationdate, 
                         quantity, 
                         purchaseprice, 
                         amount, 
                         category
                         FROM ods_csgn_stock 
                         where 1=1"

    echo "End syncing ods_csgn_stock data information"
}


# 同步ods_csgn_selling_price数据
# 同步策略 - 全量
sync_ods_csgn_selling_price_data() {
    echo "Start syncing ods_csgn_selling_price data information"
    sync_data_pg "ods_csgn_selling_price" "SELECT 
                                           division, 
                                           sapid, 
                                           dealername, 
                                           dealertype, 
                                           markettype, 
                                           upn, 
                                           description, 
                                           BSC_StdSellPrice, 
                                           BSC_StdSellPrice_VAT, 
                                           salesstatus, 
                                           isactiveindms, 
                                           level1, 
                                           level1code, 
                                           level2, 
                                           level2code, 
                                           level3, 
                                           level3code, 
                                           level4, 
                                           level4code, 
                                           level5, 
                                           level5code, 
                                           productname, 
                                           startdate, 
                                           enddate
                                           FROM ods_csgn_selling_price 
                                           where 1=1"

    echo "End syncing ods_csgn_selling_price data information"
}

# 同步ods_csgn_detail数据
# 同步策略 - 全量
sync_ods_csgn_detail_data() {
    echo "Start syncing ods_csgn_detail data information"
    sync_data_pg "ods_csgn_detail" "SELECT 
                                    divisionid, 
                                    division, 
                                    customersite, 
                                    customernumber, 
                                    customername, 
                                    material, 
                                    materialdescription, 
                                    productlineid2, 
                                    productline2, 
                                    productlineid3, 
                                    productline3, 
                                    productlineid4, 
                                    productline4, 
                                    batch, 
                                    expirationdate, 
                                    quantity, 
                                    purchaseprice, 
                                    amount, 
                                    category
                                    FROM ods_csgn_detail 
                                    where 1=1"

    echo "End syncing ods_csgn_detail data information"
}


# 同步ods_clear_stock数据
# 同步策略 - 全量
sync_ods_clear_stock_data() {
    echo "Start syncing ods_clear_stock data information"
    sync_data_pg "ods_clear_stock" "SELECT 
                                    poh_id, 
                                    ordertype, 
                                    sapcode, 
                                    dealername, 
                                    orderstatus, 
                                    createdate, 
                                    productlinename, 
                                    upn, 
                                    upnname, 
                                    requiredqty, 
                                    cfnprice, 
                                    amount
                                    FROM ods_clear_stock 
                                    where 1=1"

    echo "End syncing ods_clear_stock data information"
}

# 同步ods_csgn_stock_dms数据
# 同步策略 - 全量
sync_ods_csgn_stock_dms_data() {
    echo "Start syncing ods_csgn_stock_dms data information"
    sync_data_pg "ods_csgn_stock_dms" "SELECT 
                                    division, 
                                    divisionid, 
                                    ownerid, 
                                    ownername, 
                                    ownerparentsapid, 
                                    ownerparentdealername, 
                                    subbu, 
                                    ownertype, 
                                    ownertypealt, 
                                    ownersalestype, 
                                    markettype, 
                                    locationid, 
                                    locationname, 
                                    locationdealertype, 
                                    locationdealertypealt, 
                                    locationparentsapid, 
                                    locationparentdealer, 
                                    upn, 
                                    crmcode, 
                                    upn_description, 
                                    level1desc, 
                                    level2desc, 
                                    level3desc, 
                                    level4desc, 
                                    level5desc, 
                                    lot, 
                                    expdate, 
                                    expyear, 
                                    expmonth, 
                                    aging, 
                                    invamtbydearlerstdpurprice, 
                                    bsc_stdsellprice, 
                                    qty, 
                                    inv_amt_by_bsc_stdsellprice, 
                                    year, 
                                    month, 
                                    inventorycategory, 
                                    inventorytypename, 
                                    bicode, 
                                    biname, 
                                    formnbr, 
                                    productline1, 
                                    productline2, 
                                    productline3, 
                                    productline4, 
                                    productline5, 
                                    customizedgroup, 
                                    productgroup, 
                                    pcs, 
                                    qr_qty, 
                                    un_qr_qty, 
                                    warehouse, 
                                    warehousetype, 
                                    transferdate, 
                                    transferdays, 
                                    receiptdate, 
                                    receiptdays, 
                                    equipflag, 
                                    ordertype, 
                                    hospitalcode
                                    FROM ods_csgn_stock_dms 
                                    where 1=1"

    echo "End syncing ods_csgn_stock_dms data information"
}

# 同步ods_csgn_stock_t2数据
# 同步策略 - 全量
sync_ods_csgn_stock_t2_data() {
    echo "Start syncing ods_csgn_stock_t2 data information"
    sync_data_pg "ods_csgn_stock_t2" "SELECT 
                                    dma_sap_code, 
                                    dma_chinesename, 
                                    dma_dealertype, 
                                    nbr, 
                                    salesdate, 
                                    remark, 
                                    divisioncode, 
                                    divisionname, 
                                    lot, 
                                    upn, 
                                    qrcode, 
                                    qty, 
                                    unitprice, 
                                    consignmenttype, 
                                    salesoutnbr, 
                                    ordertype, 
                                    parentsapcode, 
                                    parentdealername
                                    FROM ods_csgn_stock_t2 
                                    where 1=1"

    echo "End syncing ods_csgn_stock_t2 data information"
}

# 同步ods_appendix_sold_to数据
# 同步策略 - 全量
sync_ods_appendix_sold_to_data() {
    echo "Start syncing ods_appendix_sold_to data information"
    sync_data_pg "ods_appendix_sold_to" "SELECT 
                                    sold_to, 
                                    customer
                                    FROM ods_appendix_sold_to 
                                    where 1=1"

    echo "End syncing ods_appendix_sold_to data information"
}

# 同步ods_appendix_productline数据
# 同步策略 - 全量
sync_ods_appendix_productline_data() {
    echo "Start syncing ods_appendix_productline data information"
    sync_data_pg "ods_appendix_productline" "SELECT 
                                    productlinename, 
                                    bu, 
                                    deadline
                                    FROM ods_appendix_productline 
                                    where 1=1"

    echo "End syncing ods_appendix_productline data information"
}

按业务分类同步数据
case $1 in
"csgn")
    --sync_ods_csgn_stock_data
    --sync_ods_csgn_selling_price_data
    --sync_ods_csgn_detail_data
    --sync_ods_clear_stock_data
    --sync_ods_csgn_stock_dms_data
    --sync_ods_csgn_stock_t2_data
    sync_ods_appendix_sold_to_data
    --sync_ods_appendix_productline_data
    ;;
*)
    echo "plesase use demo!"
    ;;
esac


# if [ "$1"x = "demo"x ];then
# 	echo "name is ok $1"
# 	sync_demo_data 
    
# else
#     echo "plesase use wo_qrcode!"
    
# fi