
n:
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
    sync_date=$(date -d '+1 day' +%F)
fi

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
# 同步销售发货单
sync_putaway_info() {
    sync_data_sqlserver "putaway" "SELECT 
                             UpdateDT
                            ,Invoice
                            ,DeliveryNo
                            ,PutAwayDate
                            ,UPN
                            ,QTY
                            ,Batch
                            ,Plant
                            ,SL
                            ,Unit
                            ,FromSAPLocation
                            ,SAPLocation
                        FROM TRANS_PutAway
                        --where format(UpdateDT, 'yyyy-MM-dd')='$sync_date'
						where format(PutAwayDate,'yyyy')='2022' "
											}
# 按业务分类同步数据
if [ "$1"x = "putaway"x ];then
	echo "$1 only run"
	sync_putaway_info 
else
    echo "wo_qrcode!  le_srr all run"

fi    

# 设置必要的参
target_db_name='opsdw'
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#  1.业务数据SQL
#Load data into table ods_putaway_info from hdfs putaway by partition
putawy_sql="
load data inpath '/bsc/origin_data/$origin_db_name/putaway/$sync_date' overwrite
into table ${target_db_name}.ods_putaway_info
partition(dt='$sync_date');
"

# 2. 执行加载数据SQL

$hive -e"$putawy_sql"

echo "End loading data on {$sync_date} ..hdfs to ods_putaway_info........................................................"


echo "start syncing so dn data into DWD layer on ${sync_date} .................."
###############################################ods_dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
# 设置必要的参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 1 Hive SQL string
so_sql="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table dwd_putaway_info partition(dt)
select 
    replace(invoice, '\'', '') as invoice,
    delivery_no, 
    putaway_date, 
    upn,
    qty,
    batch,
    plant,
    sl,
    unit,
    from_slocation,
    to_location,
    update_dt,
    date_format(putaway_date,'yyyy-MM-dd') as dt
from ods_putaway_info
where dt='$sync_date' and putaway_date is not null
"
# 2. 执行SQL
if [ "$1"x = "putaway"x ];then
	echo "$1 only run"
	$hive -e "$so_sql"
else
    echo "dwd_putaway_info failed"
fi 
echo "End syncing so dn data into DWD layer on ${sync_date} .................." 
