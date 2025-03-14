#!/bin/bash
# Function:
#   sync up PG data to HDFS template
# History:
#   2021-11-08    Donny   v1.0    init

# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
if [ -n "$2" ] ;then 
    sync_db=$2
else
   echo 'please input 第二个变量 the PostgreSQL db to be synced!'
   exit 1
fi

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

# 按业务分类同步数据
# case $1 in
# "demo")
#     sync_demo_data
#     ;;
# *)
#     echo "plesase use demo!"
#     ;;
# esac


if [ "$1"x = "demo"x ];then
	echo "name is ok $1"
	sync_demo_data 
    
else
    echo "plesase use wo_qrcode!"
    
fi