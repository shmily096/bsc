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
sync_inventory_movement() {
    sync_data_sqlserver "movement" "SELECT
                            distinct
							'' as UpdateDT
                            ,MvtType
                            ,ReasonCode
                            ,SpecialStock
                            ,MaterialDoc
                            ,MatItem
                            ,StockLoc
                            ,Plant
                            ,Material
                            ,Batch
                            ,Quantity
                            ,SLEDBBD
                            ,PostingDate
                            ,[Time]
                            ,UserName
                            ,Delivery
                            ,SAPPONumber
                            ,PoItem
                            ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (HeaderText , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as HeaderText
                            ,REPLACE (REPLACE (REPLACE (REPLACE ( REPLACE (OriginalReference , CHAR ( 10 ), '' ), CHAR ( 13 ), '' ),CHAR ( 10 ) + CHAR ( 13 ),''),CHAR ( 9 ),''),CHAR ( 32 ),'') as OriginalReference
                            ,EnterDate
                        FROM TRANS_InventoryTransactions
                        where EnterDate >='2022-01-01'"
}
# 按业务分类同步数据
if [ "$1"x = "so_dn"x ];then
	echo "$1 only run"
	sync_inventory_movement 
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
mov_sql="
load data inpath '/bsc/origin_data/$origin_db_name/movement/$sync_date' overwrite
into table ${target_db_name}.ods_inventory_movement_trans
partition(dt='$sync_date');
"

# 2. 执行加载数据SQL

$hive -e"$mov_sql"

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
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_fact_inventory_movement_trans partition(dt)
select  
		mov.movement_type
       ,mov.reason_code
       ,mov.special_stock
       ,mov.material_doc
       ,mov.mat_item
       ,mov.stock_location
       ,mov.plant
       ,mov.material
       ,mov.batch
       ,mov.qty
       ,mov.sle_dbbd
       ,mov.posting_date
       ,mov.mov_time
       ,mov.user_name
       ,mov.delivery_no
       ,mov.po_number
       ,mov.po_item
       ,mov.header_text
       ,mov.original_reference
       ,mov.enter_date
       ,date_format(mov.enter_date,'yyyy-MM-dd') as dt
from ${target_db_name}.ods_inventory_movement_trans mov
where dt='${sync_date}' and MONTH(enter_date)>=1;
"
# 2. 执行SQL
if [ "$1"x = "so_dn"x ];then
	echo "$1 only run"
	$hive -e "$sto_sql"
else
    echo "dwd_putaway_info failed"
fi 
echo "End syncing so dn data into DWD layer on ${sync_date} .................." 

