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
dwd_dim_material_adm_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_material_adm"  # HDFS 文件路径
    tablename='dwd_dim_material_adm_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_material_adm_del" #PG 删除数据的存储过程
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
dws_iekpi_e2e_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_iekpi_e2e"  # HDFS 文件路径
    tablename='dws_iekpi_e2e_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_iekpi_e2e_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_all_kpi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_all_kpi"  # HDFS 文件路径
    tablename='dwd_dim_all_kpi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_all_kpi_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_outbound_distribution_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_outbound_distribution"  # HDFS 文件路径
    tablename='dwd_outbound_distribution_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_outbound_distribution_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_dsr_le_srr_test_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_dsr_le_srr_test"  # HDFS 文件路径
    tablename='dwd_dim_dsr_le_srr_test_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_dsr_le_srr_test_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_kpi_dwd_fact_import_export_dn_info_pdt_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_kpi_dwd_fact_import_export_dn_info_pdt"  # HDFS 文件路径
    tablename='dws_kpi_dwd_fact_import_export_dn_info_pdt_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_kpi_dwd_fact_import_export_dn_info_pdt_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwt_networkswitch_qtycompare_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_networkswitch_qtycompare"  # HDFS 文件路径
    tablename='dwt_networkswitch_qtycompare_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_networkswitch_qtycompare_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwt_networkswitch_upn_list_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_networkswitch_upn_list"  # HDFS 文件路径
    tablename='dwt_networkswitch_upn_list_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_networkswitch_upn_list_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwt_finance_monthly_cs_freight_ocgs_htm_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwt_finance_monthly_cs_freight_ocgs_htm"  # HDFS 文件路径
    tablename='dwt_finance_monthly_cs_freight_ocgs_htm_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwt_finance_monthly_cs_freight_ocgs_htm_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_upn_qty_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_upn_qty"  # HDFS 文件路径
    tablename='dws_finance_upn_qty_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_upn_qty_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dwd_dim_cfda_upn_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dim_cfda_upn"  # HDFS 文件路径
    tablename='dwd_dim_cfda_upn_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dim_cfda_upn_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推4月以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_dsr_billed_cr_qty_netvalue_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dsr_billed_cr_qty_netvalue_detail"  # HDFS 文件路径
    tablename='dws_dsr_billed_cr_qty_netvalue_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dsr_billed_cr_qty_netvalue_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_net_sales_cogs_gap_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_net_sales_cogs_gap"  # HDFS 文件路径
    tablename='dws_finance_net_sales_cogs_gap_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_net_sales_cogs_gap_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_cogs_std_cogs_at_standard_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_cogs_std_cogs_at_standard_detail"  # HDFS 文件路径
    tablename='dws_finance_cogs_std_cogs_at_standard_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_cogs_std_cogs_at_standard_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_cogs_std_cogs_at_standard_gap_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_cogs_std_cogs_at_standard_gap"  # HDFS 文件路径
    tablename='dws_finance_cogs_std_cogs_at_standard_gap_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_cogs_std_cogs_at_standard_gap_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_cogs_sharing_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_cogs_sharing_detail"  # HDFS 文件路径
    tablename='dws_finance_cogs_sharing_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_cogs_sharing_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_eeo_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_eeo_detail"  # HDFS 文件路径
    tablename='dws_eeo_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_eeo_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_eeo_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_eeo"  # HDFS 文件路径
    tablename='dws_eeo_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_eeo_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
dws_finance_ocgs_inventory_charges_detail_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_finance_ocgs_inventory_charges_detail"  # HDFS 文件路径
    tablename='dws_finance_ocgs_inventory_charges_detail_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_finance_ocgs_inventory_charges_detail_del" #PG 删除数据的存储过程    
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data_two "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}
# 同步DEMO数据
ods_aws_inbound_txn_bsc_psi_data() {
    echo "Start export demo data information"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/ods_aws_inbound_txn_bsc_psi"  # HDFS 文件路径
    tablename='ods_aws_inbound_txn_bsc_psi_merge' #PG 处理数据的存储过程  
    #清空全部数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="ods_aws_inbound_txn_bsc_psi_del" #PG 删除数据的存储过程    
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
"dws_dsr_billed_daily")
    export_demo_data
    ;;
"dws_ie_kpi")
    dws_ie_kpi_data
    ;;
"dwt_kpi_by_bu_detail")
    dwt_kpi_by_bu_detail_data
    ;;
"ods_aws_inbound_txn_bsc_psi")
    ods_aws_inbound_txn_bsc_psi_data
    ;;
"dws_finance_ocgs_inventory_charges_detail")
    dws_finance_ocgs_inventory_charges_detail_data
    ;;
"dws_eeo")
    dws_eeo_data
    ;;
"dws_eeo_detail")
    dws_eeo_detail_data
    ;;
"dws_finance_cogs_sharing_detail")
    dws_finance_cogs_sharing_detail_data
    ;;
"dws_finance_cogs_std_cogs_at_standard_detail")
    dws_finance_cogs_std_cogs_at_standard_detail_data
    ;;
"dws_finance_cogs_std_cogs_at_standard_gap")
    dws_finance_cogs_std_cogs_at_standard_gap_data
    ;;
"dws_finance_net_sales_cogs_gap")
    dws_finance_net_sales_cogs_gap_data
    ;;
"dws_dsr_billed_cr_qty_netvalue_detail")
    dws_dsr_billed_cr_qty_netvalue_detail_data
    ;;
"dwd_dim_cfda_upn")
    dwd_dim_cfda_upn_data
    ;;
"dws_finance_upn_qty")
    dws_finance_upn_qty_data
    ;;
"dwt_finance_monthly_cs_freight_ocgs_htm")
    dwt_finance_monthly_cs_freight_ocgs_htm_data
    ;;
"dwt_networkswitch_upn_list")
    dwt_networkswitch_upn_list_data
    ;;
"dwt_networkswitch_qtycompare")
    dwt_networkswitch_qtycompare_data
    ;;
"dws_kpi_dwd_fact_import_export_dn_info_pdt")
    dws_kpi_dwd_fact_import_export_dn_info_pdt_data
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
"dwd_dim_material_adm")
    dwd_dim_material_adm_data
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
"dws_iekpi_e2e")
    dws_iekpi_e2e_data
    ;;
"dwd_dim_all_kpi")
    dwd_dim_all_kpi_data
    ;;
"dwd_outbound_distribution")
    dwd_outbound_distribution_data
    ;;
"dwd_dim_dsr_le_srr_test")
    dwd_dim_dsr_le_srr_test_data
    ;;
"all")
    echo "plesase use true 参数!"
    ;;
*)
    echo "plesase use dws_dsr_billed_daily!"
    ;;
esac