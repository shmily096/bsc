#!/bin/bash
########第一阶段 从pg到hdfs
# 1 设置sqoop工具路径
sqoop="/opt/module/sqoop/bin/sqoop"

# 2 设置同步的数据库
if [ -n "$2" ] ;then 
    sync_db=$2
else
    sync_db='tableaudb'
   # echo 'please input 第二个变量 the PostgreSQL db to be synced!'
   # exit 1
fi

# 3 设置数据库连接字符串
connect_str_pg="jdbc:postgresql://10.226.98.58:55433/$sync_db"
#59的也读58的pg保证数据源一致

# 4 同步日期设置，默认同步当天数据
if [ -n "$3" ]; then
    sync_date=$3
else
    sync_date=$(date  +%F)
fi
old_date1="$(date -d "1 day ago" +'%F')"
#5 DB User&Password -- TO be udpated
user='postgres'
pwd='1qazxsw2'
CC="'"         #这个是为了给日期加上单引号如果不加到hdfs就没有lzo.index
sync_sto_pg() {
    echo "${sync_date} stat syncing........"
    $sqoop import \
        --connect $connect_str_pg\
        --username $user \
        --password $pwd \
        --target-dir /bsc/origin_data/$sync_db/$1/$sync_date \
        --delete-target-dir \
        --query "$2 and \$CONDITIONS" \
		--hive-drop-import-delims \
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

sync_offline_transaction_attribute() {
    echo "Start syncing offline_transaction data information"
    sync_sto_pg "offline_transaction" "SELECT
                                            division, 
                                            net_billed,
                                            material,
                                            bill_date, 
                                            bill_qty, 
                                            billed_rebate,
                                            so_no, 
                                            customer_code,
                                            now () as updatetime
                                FROM public.offline_transaction
                                            where 1=1"
	echo "End syncing offline_transaction data information"
}
if [ "$1"x = "offline_transaction"x ];then
	echo "$1 only run"
	echo "$old_date1  ok"
	sync_offline_transaction_attribute 

else
    echo "参数错误"
fi
##############################################第二阶段hdfs to ods
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='tableaudb' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
master_sql=""
ods_offline_transaction="
 load data inpath '/bsc/origin_data/tableaudb/offline_transaction/$sync_date' overwrite
into table opsdw.ods_offline_transaction 
;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/offline_transaction/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$ods_offline_transaction"
fi

# 2. 执行加载数据SQL
if [ "$1"x = "offline_transaction"x ];then
	echo "ODS $1 only run"	
	$hive -e "$ods_offline_transaction"
	echo "ODS  finish ods_offline_transaction data into ODS layer on ${sync_date} .................."
else 
    echo "参数错误"
fi
########################################第三阶段 ods to dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
dwd_dim_offline_transaction="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_dim_offline_transaction partition(bill_date)
SELECT
    division, 
    net_billed,
    material,     
    bill_qty, 
    billed_rebate,
    so_no, 
    customer_code,
    now () as updatetime,
    bill_date
from ${target_db_name}.ods_offline_transaction
where month(bill_date)>=1
;
"
if [ "$1"x = "offline_transaction"x ];then
	echo "$dwd_dim_offline_transaction"	
	$hive -e "$dwd_dim_offline_transaction"
	echo "DWD  finish dwd_dim_offline_transaction data into DWD layer on ${sync_date} .................."

else 
    echo "参数错误"
fi