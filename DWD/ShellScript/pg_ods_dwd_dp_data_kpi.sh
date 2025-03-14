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
old_date1="$(date -d "3 day ago" +'%F')"
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
sync_kpi_dpchinadatabase_kpi() {
    echo "Start syncing kpi_dpchinadatabase_kpi data information"
    sync_sto_pg "kpi_dpchinadatabase_kpi" "SELECT 
                                            "content", div, date_amount, updatedt, "year", date_name
                    FROM public.kpi_dpchinadatabase_kpi 
                    where updatedt='$sync_date'"
	echo "End syncing plant master data information"
}
sync_kpi_dpchinadatabase_data() {
    echo "Start syncing kpi_dpchinadatabase_data data information"
    sync_sto_pg "kpi_dpchinadatabase_data" "SELECT
                                            "content" || target||div||"year" as xx,
                                            display_uom,
                                            year, 
                                            combination,
                                            div,
                                            content,
                                            target, 
                                            date_name,
                                            date_amount,
                                            updatedt
                                            FROM public.kpi_dpchinadatabase_data
                                            where updatedt='$sync_date'"
	echo "End syncing plant master data information"
}
if [ "$1"x = "kpi_dpchinadatabase_kpi"x ];then
	echo "$1 only run"
	echo "$old_date1  ok"
	sync_kpi_dpchinadatabase_kpi 
elif [ "$1"x = "kpi_dpchinadatabase_data"x ];then
    echo " ODS $1 only run"
	echo "$sync_date  ok"
	sync_kpi_dpchinadatabase_data
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
kpi_dpchinadatabase_kpi="
 load data inpath '/bsc/origin_data/tableaudb/kpi_dpchinadatabase_kpi/$sync_date' overwrite
into table opsdw.ods_kpi_dpchinadatabase_kpi partition(dt='$sync_date')
;"

hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/kpi_dpchinadatabase_kpi/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$kpi_dpchinadatabase_kpi"
fi
kpi_dpchinadatabase_data="
load data inpath '/bsc/origin_data/$origin_db_name/kpi_dpchinadatabase_data/$sync_date' overwrite
into table ${target_db_name}.ods_kpi_dpchinadatabase_data partition(dt='$sync_date');
"
hdfs dfs -test -d "/bsc/origin_data/$origin_db_name/kpi_dpchinadatabase_data/$sync_date"
if [ $? -eq 0 ];
then
	echo " diaoyong ok"
    master_sql="$master_sql""$kpi_dpchinadatabase_data"
fi
# 2. 执行加载数据SQL
if [ "$1"x = "kpi_dpchinadatabase_kpi"x ];then
	echo "ODS $1 only run"	
	$hive -e "$kpi_dpchinadatabase_kpi"
	echo "ODS  finish kpi_dpchinadatabase_kpi data into ODS layer on ${sync_date} .................."
elif [ "$1"x = "kpi_dpchinadatabase_data"x ];then
    echo " ODS $1 only run"
	echo "$sync_date  ok"
	$hive -e "$kpi_dpchinadatabase_data"
	echo "ODS  finish kpi_dpchinadatabase_data data into ODS layer on ${sync_date} .................."
else 
    echo "参数错误"
fi
########################################第三阶段 ods to dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
dp_dwd="
use ${target_db_name};
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table opsdw.dwd_kpi_dpchinadatabase_data_and_kpi partition(year_mon)
SELECT
 xx.alll
,xx.divs
,xx.target
,xx.years
,xx.date_amount
,xx.combination
,xx.content
,xx.date_name
,xx.updatedt
,xx.display_uom
,xx.stream
,xx.table_name
,yy.year_mon
FROM (
    SELECT 
        alll,
        divs,
        target,
        years,
        date_amount,
        combination,
         content,
        date_name,
        updatedt,
        display_uom,
          'DP' AS stream,
          'DATA' AS table_name
    FROM ods_kpi_dpchinadatabase_data
    where dt='$sync_date'
    union all
     SELECT  
         '' as alll,
         divs,
         '' as  target,
         year as years,
         date_amount,
        ''AS combination,
          content,
          date_name,
          updatedt,
          '' as display_uom,
          'DP' AS stream,
          'kpi' AS table_name
    FROM ods_kpi_dpchinadatabase_kpi 
    where dt='$sync_date' )xx
    left join (SELECT max(date_name)year_mon FROM ods_kpi_dpchinadatabase_data
where content='Commercial Sales' and target='Act./Proj.' and divs='Total China'
and date_amount>0 and substr(date_name,1,1)='2' and dt='$sync_date' )yy on 1=1;
"
if [ "$1"x = "dp_dwd"x ];then
	echo "$dp_dwd"	
	$hive -e "$dp_dwd"
	echo "DWD  finish dp_dwd data into DWD layer on ${sync_date} .................."
    sh /bscflow/ods/remove_ods_dp.sh
else 
    echo "参数错误"
fi