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

sync_dwd_sap_Warehouse_attribute() {
    echo "Start syncing dwd_sap_Warehouse_attribute data information"
    sync_sto_pg "dwd_sap_Warehouse_attribute" "SELECT 
                                                    name,
                                                    plant, 
                                                    location, 
                                                    status, 
                                                    sap_status,
                                                    product_quality_status,
                                                    warehouse, 
                                                    normal_or_abnormal,
                                                    supplier,
                                                    dt
                                                    FROM public.storage_location
                                            where dt='$old_date1'"
	echo "End syncing dwd_sap_Warehouse_attribute data information"
}
sync_dwd_combination_rebate_attribute() {
    echo "Start syncing combination_rebate data information"
    sync_sto_pg "combination_rebate" "SELECT 
                                                    product_level,
                                                    bu, 
                                                    rate,
                                                    cust_business_type, 
                                                    type, 
                                                    combo_group, 
                                                    start_date,
                                                    end_date,
                                                    current_date
                                            FROM public.combination_rebate
                                            where 1=1
                                            "
	echo "End syncing dwd_combination_rebate data information"
}
if [ "$1"x = "dwd_sap_Warehouse_attribute"x ];then
	echo "$1 only run"
	echo "$old_date1  ok"
	sync_dwd_sap_Warehouse_attribute 
elif [ "$1"x = "combination_rebate"x ];then
    echo " HDFS $1 only run"
	echo "$sync_date  ok"
	sync_dwd_combination_rebate_attribute
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
ods_sap_Warehouse_attribute="
 load data inpath '/bsc/origin_data/tableaudb/dwd_sap_Warehouse_attribute/$sync_date' overwrite
into table opsdw.ods_sap_Warehouse_attribute 
;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/dwd_sap_Warehouse_attribute/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$ods_sap_Warehouse_attribute"
fi
ods_combination_rebate="
 load data inpath '/bsc/origin_data/tableaudb/combination_rebate/$sync_date' overwrite
into table opsdw.ods_combination_rebate partition(dt='$sync_date')
;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/combination_rebate/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$ods_combination_rebate"
fi
# 2. 执行加载数据SQL
if [ "$1"x = "dwd_sap_Warehouse_attribute"x ];then
	echo "ODS $1 only run"	
	$hive -e "$ods_sap_Warehouse_attribute"
	echo "ODS  finish ods_sap_Warehouse_attribute data into ODS layer on ${sync_date} .................."
elif [ "$1"x = "combination_rebate"x ];then
    echo " ODS $1 only run"
	echo "$sync_date  ok"
	$hive -e "$ods_combination_rebate"
    echo "ODS  finish ods_combination_rebate data into ODS layer on ${sync_date} .................."
else 
    echo "参数错误"
fi
########################################第三阶段 ods to dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
kpi_dwd_sap_Warehouse_attribute="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_sap_Warehouse_attribute partition(dt)
SELECT
    replace(name,' ','') as name, 
    plant, 
    location, 
    status,
    sap_status,
    p_quality_status, 
    chinis_name, 
    flag, 
    supplier,
    dt
from ${target_db_name}.ods_sap_Warehouse_attribute
where dt='${old_date1}'
;
"
dwd_combination_rebate="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_combination_rebate 
SELECT
    product_level,
    bu, 
    rate,
    cust_business_type, 
    type, 
    combo_group, 
    start_date,
    end_date,
    dt
from ${target_db_name}.ods_combination_rebate
where dt='${sync_date}' and   substr(start_date,1,1)='2'
;
"
if [ "$1"x = "dwd_sap_Warehouse_attribute"x ];then
	echo "$kpi_dwd_sap_Warehouse_attribute"	
	$hive -e "$kpi_dwd_sap_Warehouse_attribute"
	echo "DWD  finish dwd_sap_Warehouse_attribute data into DWD layer on ${sync_date} .................."
elif [ "$1"x = "combination_rebate"x ];then
    echo " DWD $1 only run"
	echo "$sync_date  ok"
	$hive -e "$dwd_combination_rebate"
    echo "DWD  finish dwd_combination_rebate data into DWD layer on ${sync_date} .................."
else 
    echo "参数错误"
fi