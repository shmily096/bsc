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
# 同步dws_dq_exception数据
dws_dq_exception_data() {
    echo "Start export data dws_dq_exception"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dws_dq_exception"  # HDFS 文件路径
    tablename='dws_dq_exception_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dws_dq_exception_del" #PG 删除数据的存储过程
    export_data $demo_pg_funcion $demo_export_dir
    #插入数据全量更新 暂时是推23年及以后的数据
    export_data "$tablename" "$NEW_export_dir" 
    echo "End syncing plant master data information"
}

# 同步dwd_dq_exception数据
dwd_dq_exception_data() {
    echo "Start export data dwd_dq_exception"
    #export path table by table
    NEW_export_dir="/bsc/opsdw/export/dwd_dq_exception"  # HDFS 文件路径
    tablename='dwd_dq_exception_merge' #PG 处理数据的存储过程    
    #清空最近31天的数据
    demo_export_dir="/bsc/opsdw/export/ads_demo"  # HDFS 文件路径
    demo_pg_funcion="dwd_dq_exception_del" #PG 删除数据的存储过程
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
    ;;
"dq")
    dws_dq_exception_data
    dwd_dq_exception_data
    ;;
"dwd")
    dwd_dq_exception_data
    ;;
"all")
    echo "plesase use true 参数!"
    ;;
*)
    echo "plesase use dws_dsr_billed_daily!"
    ;;
esac