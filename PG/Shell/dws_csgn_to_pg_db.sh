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
    pg_db='tableaudb'
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
dwd_trans_csgn_clear_data() {
    echo "Start export data dwd_trans_csgn_clear"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_trans_csgn_clear"  # HDFS 文件路径
    tablename='dwd_trans_csgn_clear_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_trans_csgn_clear_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_trans_csgn_t2_data() {
    echo "Start export data dwd_trans_csgn_t2"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_trans_csgn_t2"  # HDFS 文件路径
    tablename='dwd_trans_csgn_t2_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_trans_csgn_t2_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_trans_salesdealerinventory_data() {
    echo "Start export data dwd_trans_salesdealerinventory"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_trans_salesdealerinventory"  # HDFS 文件路径
    tablename='dwd_trans_salesdealerinventory_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_trans_salesdealerinventory_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_trans_consignmenttracking_data() {
    echo "Start export data dwd_trans_consignmenttracking"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_trans_consignmenttracking"  # HDFS 文件路径
    tablename='dwd_trans_consignmenttracking_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_trans_consignmenttracking_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_csgnturn_over_rate_data() {
    echo "Start export data dwd_csgnturn_over_rate"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_csgnturn_over_rate"  # HDFS 文件路径
    tablename='dwd_csgnturn_over_rate_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_csgnturn_over_rate_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_trans_consignmentlist_data() {
    echo "Start export data dwd_trans_consignmentlist"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_trans_consignmentlist"  # HDFS 文件路径
    tablename='dwd_trans_consignmentlist_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_trans_consignmentlist_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dws_fact_sales_order_invoice_data() {
    echo "Start export data dws_fact_sales_order_invoice"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_fact_sales_order_invoice"  # HDFS 文件路径
    tablename='dws_fact_sales_order_invoice_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_fact_sales_order_invoice_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_otds_itemlevel_data() {
    echo "Start export data dwd_otds_itemlevel"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_otds_itemlevel"  # HDFS 文件路径
    tablename='dwd_otds_itemlevel_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_otds_itemlevel_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

dwd_hk_expiration_data() {
    echo "Start export data dwd_hk_expiration"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_hk_expiration"  # HDFS 文件路径
    tablename='dwd_hk_expiration_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_hk_expiration_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}


# 同步DEMO数据
dwt_kpi_by_bu_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_kpi_by_bu_detail"  # HDFS 文件路径
    tablename='dwt_kpi_by_bu_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_kpi_by_bu_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 按业务分导出数据
case $1 in
"csgn_clear")
    dwd_trans_csgn_clear_data
    dwd_trans_csgn_t2_data
    dwd_trans_salesdealerinventory_data
    dwd_trans_consignmenttracking_data
    dwd_csgnturn_over_rate_data
    dwd_trans_consignmentlist_data
    dws_fact_sales_order_invoice_data
    dwd_otds_itemlevel_data
    dwd_hk_expiration_data
    ;;
"otds_itemlevel")
    dwd_otds_itemlevel_data
    ;;
"clear")
    dwd_trans_csgn_t2_data
    ;;
"csgn_tracking")
    dwd_trans_consignmenttracking_data
    ;;
"csgn_turn")
    dwd_csgnturn_over_rate_data
    ;;
"csgn_list")
    dwd_trans_consignmentlist_data
    ;;
"csgn_invoice")
    dws_fact_sales_order_invoice_data
    ;;
"dwt_kpi_by_bu_detail")
    dwt_kpi_by_bu_detail_data
    ;;
"hk")
    dwd_hk_expiration_data
    ;;
"all")
    echo "plesase use true 参数!"
    ;;
*)
    echo "plesase use dws_dsr_billed_daily!"
    ;;
esac