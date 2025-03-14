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


# 同步Plant主数据
# 同步策略 - 全量
sync_plant_master_data() {
    echo "Start syncing plant master data information"
    sync_data_sqlserver "plant_sway" "select PlantCode,
                                        PostlCode,
                                        City,
                                        Name2,
                                        Name1
                                        from MDM_Plant
                                        where 1=1"

    echo "End syncing plant master data information"

}

# 按业务分类同步数据
case $1 in
"plant")
    sync_plant_master_data
    ;;
"master_data")
    sync_plant_master_data
    sync_location_master_data
    sync_material_master_data
    sync_calendar_master_data
    ;;
*)
    echo "plesase use master_data, trans, all!"
    ;;
esac
