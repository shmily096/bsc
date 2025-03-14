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
    #sync_date='2024-10-21'
    year_month=$(date  +'%Y-%m')
    last_month=$(date -d "$(date  +'%Y%m')01 last month" +'%Y%m')
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

#同步PostgreSQL数据通过sqoop \001
sync_data001_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
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


# 同步ods_dq_marc数据
# 同步策略 - 全量
sync_ods_perfectorder_controllist() {
    echo "Start syncing perfectorder_controllist data information"
    sync_data001_pg "ods_perfectorder_controllist" "SELECT 
                                                    \"Updatedt\", 
                                                    \"Yrmondate\", 
                                                    \"FileDate\", 
                                                    \"Yrmon\", 
                                                    \"0-SalesDocument\", 
                                                    \"1-BU\", 
                                                    \"Line\", 
                                                    \"2-UPN\", 
                                                    \"2-UPN/Model\", 
                                                    \"Multi-Dash\", 
                                                    \"Suggest Dash (CRM Only)\", 
                                                    \"3-PL3\", 
                                                    \"4-PL4\", 
                                                    \"5-PL5\", 
                                                    \"6-CustomerType\", 
                                                    \"7-CustomerID\",
                                                     \"8-OrderType\", 
                                                     \"Currency\", 
                                                     \"Sales Org\", 
                                                     \"Purchase Order Number\", 
                                                     \"Batch\", 
                                                     \"Plant\", 
                                                     \"Storage Location\", 
                                                     \"Order Qty\", 
                                                     \"User Name\", 
                                                     \"Net Value\", 
                                                     \"Customer Name\", 
                                                     \"SO Interface Type\", 
                                                     \"Reject Reason\", 
                                                     \"Head Block\", 
                                                     \"Item Block\", 
                                                     \"Delivery Priority\", 
                                                     \"Line Status\", 
                                                     \"SO Status (DMS)\", 
                                                     \"OrderFlag\", 
                                                     \"F1_Outbound\", 
                                                     \"F2_Division\", 
                                                     \"F3_ReplenishType\", 
                                                     \"Reason_permitted2issue\", 
                                                     \"Reason_noissue\", 
                                                     \"IF Control\", 
                                                     \"Next Action\", 
                                                     \"Action Type\", 
                                                     \"Days of Open\", 
                                                     \"Open Group\", 
                                                     \"Sales Document Date\", 
                                                     \"9-OrderReasonCode\", 
                                                     \"Table Names\", 
                                                     \"File Paths\"
                                                    FROM public.\"PerfectOrder_ControlList\"
                                                    where 1=1"

    echo "End syncing perfectorder_controllist data information"
}




# 同步ods_dq_mlan数据
# 同步策略 - 全量
sync_ods_perfectorder_confirmedreason() {
    echo "Start syncing ods_perfectorder_confirmedreason data information"
    sync_data001_pg "ods_perfectorder_confirmedreason" "SELECT 
                                \"UpdateDT\", 
                                targetyrmon, 
                                so_no, dn_dt, 
                                delivery_id, 
                                material, 
                                so_qty, 
                                customer_type, 
                                bu, 
                                cust_name, 
                                year_mon, 
                                casetype, 
                                adjust_casetype, 
                                \"File Paths\"
                                FROM public.\"PerfectOrder_ConfirmedReason\"
                                where 1=1"

    echo "End syncing ods_perfectorder_confirmedreason data information"
}




按业务分类同步数据
case $1 in
"controlList")
    sync_ods_perfectorder_controllist
    ;;
"confirmedReason")
    sync_ods_perfectorder_confirmedreason
    ;;
"perfect")
    sync_ods_perfectorder_controllist
    sync_ods_perfectorder_confirmedreason
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