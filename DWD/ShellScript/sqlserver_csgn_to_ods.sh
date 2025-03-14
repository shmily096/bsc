#!/bin/bash
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
fi
yesterday=$(date -d '-1 day' +%F)
this_year=`date -d "${sync_date}" +%Y-01`
#同步SQl Server数据通过sqoop
sync_data_sqlserver() {
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
# 同步销售发货单
sync_trans_openordercn_info() {
    sync_data_sqlserver "trans_openordercn" "SELECT 
                                            UpdateDT,
                                            InsertDate
                                FROM APP_OPS.dbo.TRANS_OpenOrderCN
                        where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
						 "
											}
sync_trans_delivery_intrans_info() {
    sync_data_sqlserver "trans_delivery_intrans" "SELECT 
                                                    ID, 

                                            FROM APP_OPS.dbo.TRANS_Delivery_InTrans
                                            ---where 1=1
                                    where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
						 "
											}
# 按业务分类同步数据
if [ "$1"x = "trans_openordercn"x ];then
	echo "$1 only run"
	sync_trans_openordercn_info 
elif [ "$1"x = "trans_delivery_intrans"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	sync_trans_delivery_intrans_info
else
    echo "failed run"

fi    

# 设置必要的参
target_db_name='opsdw'
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#  1.业务数据SQL
#Load data into table ods_trans_openordercn from hdfs trans_openordercn by partition
ods_trans_openordercn_sql="
load data inpath '/bsc/origin_data/$origin_db_name/trans_openordercn/$sync_date' overwrite
into table ${target_db_name}.ods_trans_openordercn
partition(dt='$sync_date');
"
ods_trans_delivery_intrans_sql="
load data inpath '/bsc/origin_data/$origin_db_name/trans_delivery_intrans/$sync_date' overwrite
into table ${target_db_name}.ods_trans_delivery_intrans
partition(dt='$sync_date');
"
# 2. 执行加载数据SQL
if [ "$1"x = "trans_openordercn"x ];then
	echo "$1 only run"
	$hive -e"$ods_trans_openordercn_sql"
elif [ "$1"x = "trans_delivery_intrans"x ];then
    echo " $1 only run"
	echo "$sync_date  ok"
	$hive -e"$ods_trans_delivery_intrans_sql"
else
    echo "failed run"

fi   


echo "End loading data on {$sync_date} ..hdfs to ods_trans_openordercn........................................................"
