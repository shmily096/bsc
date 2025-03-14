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


同步PostgreSQL数据通过sqoop
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

# export_data() {
#     echo "${sync_date} stat exprting........"
#     echo $2
#     $sqoop export \
#         --connect $connect_str_pg \
#         --username $user \
#         --password $pwd \
#         --table forwarder_app \
#         --num-mappers 1 \
#         --export-dir $2 \
#         --fields-terminated-by "\t" \
#         --columns "count_dt, forwarder, pick_up_median, month, year"
#         --update-mode allowinsert \
#         --update-key $3 \
#         --m 1 \
#         --input-null-string '\\N' \
#         --input-null-non-string '\\N'

#     echo "end exprting........"
# }

# 同步DEMO数据
export_demo_data() {
    echo "Start export demo data information"
    #export path table by table
    demo_export_dir="/bsc/opsdw/export/dws_dsr_billed_daily"  # HDFS 文件路径
    tablename='dws_dsr_billed_daily_merge' #PG 处理数据的存储过程
    #key_name='count_dt,forwarder,month,year'
    export_data "$tablename" "$demo_export_dir" 

    echo "End syncing plant master data information"
}
# 同步DEMO数据
export_cr_data() {
    echo "Start export demo data information"
    #export path table by table
    demo_export_dir="/bsc/opsdw/export/dws_dsr_cr_daily"  # HDFS 文件路径
    tablename='dws_dsr_cr_daily_merge' #PG 处理数据的存储过程
    #key_name='count_dt,forwarder,month,year'
    export_data "$tablename" "$demo_export_dir" 

    echo "End syncing plant master data information"
}
# 按业务分导出数据
case $1 in
"dws_dsr_billed_daily")
    export_demo_data
    ;;
"dws_dsr_cr_daily")
    export_cr_data
    ;;
"all")
    export_demo_data
    export_cr_data
    ;;
*)
    echo "plesase use dws_dsr_billed_daily!"
    ;;
esac
