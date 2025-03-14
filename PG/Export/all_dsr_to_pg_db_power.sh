#!/bin/bash
# Function:
#   sync up HDFS to PG data template
# History:
#   2021-11-09    Donny   v1.0    init

# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
if [ -n "$2" ] ;then 
    pg_db=$2
else
    pg_db='ChinaOpsPowerApps'
fi

if [ -n "$3" ]; then
    sync_date=$3
else
    sync_date=$(date  +%F)
fi


# 3 设置数据库连接字符串
connect_str_pg="jdbc:postgresql://10.226.98.58:55433/$pg_db"

#5 DB User&Password -- TO be udpated
user='postgres'
pwd='1qazxsw2'


#同步PostgreSQL数据通过sqoop
export_data() {
    echo "${sync_date} stat exprting........"
    echo $2
    $sqoop export \
        --connect $connect_str_pg \
        --username $user \
        --password $pwd \
        --call $1 \
        --export-dir $2 \
        --fields-terminated-by '\t' \
        --input-null-string '\\N' \
        --input-null-non-string '\\N'

    echo "end exprting........"
}
#同步PostgreSQL数据通过sqoop 用\001分隔
export_data_two() {
    echo "${sync_date} stat exprting........"
    echo $2
    $sqoop export \
        --connect $connect_str_pg \
        --username $user \
        --password $pwd \
        --call $1 \
        --export-dir $2 \
        --fields-terminated-by '\001' \
        --input-null-string '\\N' \
        --input-null-non-string '\\N'

    echo "end exprting........"
}
# 同步DEMO数据
export_demo_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_billed_daily"  # HDFS 文件路径
    tablename='dws_dsr_billed_daily_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_billed_daily_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据 最近31天数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
export_cr_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_cr_daily"  # HDFS 文件路径
    tablename='dws_dsr_cr_daily_merge' #PG 处理数据的存储过程  
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_cr_daily_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据  最近31天数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_customer_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_customer"  # HDFS 文件路径
    tablename='dwd_dim_customer_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_customer_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据  当天数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_material_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_material"  # HDFS 文件路径
    tablename='dwd_dim_material_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_material_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据  当天数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_dsr_dned_daily_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_dned_daily"  # HDFS 文件路径
    tablename='dws_dsr_dned_daily_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_dned_daily_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据 year>=2022 
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_dsr_fulfill_daily_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_fulfill_daily"  # HDFS 文件路径
    tablename='dws_dsr_fulfill_daily_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_fulfill_daily_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 4月开始的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_dsr_dealer_daily_transation_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_dealer_daily_transation"  # HDFS 文件路径
    tablename='dws_dsr_dealer_daily_transation_merge' #PG 处理数据的存储过程  
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_dealer_daily_transation_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据 更新最近31天数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwt_dsr_dealer_topic_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_dsr_dealer_topic"  # HDFS 文件路径
    tablename='dwt_dsr_dealer_topic_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_dsr_dealer_topic_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwt_dsr_topic_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_dsr_topic"  # HDFS 文件路径
    tablename='dwt_dsr_topic_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_dsr_topic_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_sales_waybill_timi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_sales_waybill_timi"  # HDFS 文件路径
    tablename='dws_kpi_sales_waybill_timi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_sales_waybill_timi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_monthly_isolate_stock_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_monthly_isolate_stock"  # HDFS 文件路径
    tablename='dws_kpi_monthly_isolate_stock_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_monthly_isolate_stock_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_zc_timi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_zc_timi"  # HDFS 文件路径
    tablename='dws_kpi_zc_timi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_zc_timi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_stock_putaway_time_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_stock_putaway_time"  # HDFS 文件路径
    tablename='dws_kpi_stock_putaway_time_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_stock_putaway_time_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_kpi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_kpi"  # HDFS 文件路径
    tablename='dwd_dim_kpi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_kpi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_ie_kpi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_ie_kpi"  # HDFS 文件路径
    tablename='dws_ie_kpi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_ie_kpi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_cc_so_delivery_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_cc_so_delivery"  # HDFS 文件路径
    tablename='dws_kpi_cc_so_delivery_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_cc_so_delivery_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_dsr_fulfill_monthly_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_fulfill_monthly"  # HDFS 文件路径
    tablename='dws_dsr_fulfill_monthly_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_fulfill_monthly_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_sto_migo2pgi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_sto_migo2pgi"  # HDFS 文件路径
    tablename='dws_kpi_sto_migo2pgi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_sto_migo2pgi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_iekpi_e2e_tj_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_iekpi_e2e_tj"  # HDFS 文件路径
    tablename='dwd_iekpi_e2e_tj_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_iekpi_e2e_tj_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推昨天对应的当年的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 按业务分导出数据
case $1 in
"dws_dsr_billed_daily")
    export_demo_data
    ;;
"dws_ie_kpi")
    dws_ie_kpi_data
    ;;
"dwd_iekpi_e2e_tj")
    dwd_iekpi_e2e_tj_data
    ;;
"dws_kpi_sto_migo2pgi")
    dws_kpi_sto_migo2pgi_data
    ;;
"dws_dsr_fulfill_monthly")
    dws_dsr_fulfill_monthly_data
    ;;
"dws_kpi_cc_so_delivery")
    dws_kpi_cc_so_delivery_data
    ;;
"dwd_dim_kpi")
    dwd_dim_kpi_data
    ;;
"dws_dsr_cr_daily")
    export_cr_data
    ;;
"dwd_dim_customer")
    dwd_dim_customer_data
    ;;
"dwd_dim_material")
    dwd_dim_material_data
    ;;
"dws_dsr_dned_daily")
    dws_dsr_dned_daily_data
    ;;
"dws_dsr_fulfill_daily")
    dws_dsr_fulfill_daily_data
    ;;
"dws_dsr_dealer_daily_transation")
    dws_dsr_dealer_daily_transation_data
    ;;
"dwt_dsr_dealer_topic")
    dwt_dsr_dealer_topic_data
    ;;
"dwt_dsr_topic")
    dwt_dsr_topic_data
    ;;
"dws_kpi_sales_waybill_timi")
    dws_kpi_sales_waybill_timi_data
    ;;
"dws_kpi_monthly_isolate_stock")
    dws_kpi_monthly_isolate_stock_data
    ;;
"dws_kpi_zc_timi")
    dws_kpi_zc_timi_data
    ;;
"dws_kpi_stock_putaway_time")
    dws_kpi_stock_putaway_time_data
    ;;
"all")
    export_demo_data
    export_cr_data
    dwd_dim_customer_data
    dwd_dim_material_data
    dws_dsr_dned_daily_data
    dws_dsr_fulfill_daily_data
    dws_dsr_dealer_daily_transation_data
    dwt_dsr_dealer_topic_data
    dwt_dsr_topic_data
    dws_kpi_sales_waybill_timi_data
    dws_kpi_monthly_isolate_stock_data
    dws_kpi_zc_timi_data
    dws_kpi_stock_putaway_time_data
    dwd_dim_kpi_data
    dws_ie_kpi_data
    dws_kpi_cc_so_delivery_data
    dws_dsr_fulfill_monthly_data
    dws_kpi_sto_migo2pgi_data
    ;;
*)
    echo "plesase use dws_dsr_billed_daily!"
    ;;
esac