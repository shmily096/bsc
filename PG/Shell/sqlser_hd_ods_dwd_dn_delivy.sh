
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
    sync_date=$(date  +%F)
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
sync_sales_order_dn() {
    sync_data_sqlserver "sales_order_dn" "SELECT  ID
                                                ,UpdateDT
                                                ,Active
                                                ,SONo
                                                ,SAPDeliveryNo
                                                ,DNCreateDT
                                                ,DNUpdateDT
                                                ,DNCreateBy
                                                ,DNUpdatedBy
                                                ,ShipTo
                                                ,RealShipToAddress
                                                ,DeliveryLine
                                                ,Material
                                                ,QTY
                                                ,QRCode
                                                ,Batch
                                                ,PlannedGIDate
                                                ,ActualGIDate
                                                ,DeliveryMode
                                                ,Carrier
                                                ,PickLocation
												,Plant
                                            FROM TRANS_SalesDelivery
                                           where format(DNCreateDT, 'yyyy-MM-dd')>='2022-01-01'"
											}
# 按业务分类同步数据
if [ "$1"x = "so_dn"x ];then
	echo "$1 only run"
	sync_sales_order_dn 
else
    echo "wo_qrcode!  le_srr all run"

fi    

# 设置必要的参
target_db_name='opsdw'
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

#  1.业务数据SQL
#Load data into table ods_sales_delivery from hdfs sales_order_dn by partition
so_dn_sql="
load data inpath '/bsc/origin_data/$origin_db_name/sales_order_dn/$sync_date' overwrite 
into table opsdw.ods_sales_delivery 
partition(dt='2022-05-12');"

# 2. 执行加载数据SQL

$hive -e"$so_dn_sql"

echo "End loading data on {$sync_date} ..hdfs to ods................"


echo "start syncing so dn data into DWD layer on ${sync_date} .................."
###############################################ods_dwd
export LANG="en_US.UTF-8"
echo "start syncing ODS TO DWD  layer on ${sync_date} ................."
# 设置必要的参数
target_db_name='opsdw' # 数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
# 1 Hive SQL string
dn_header_sql="
use opsdw;
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

-- Sales order delivery note header
insert overwrite table dwd_fact_sales_order_dn_info partition(dt)
select distinct 
     h_dn.so_no
    ,h_dn.sap_delivery_no --delivery_id
    ,h_dn.min_create_dt --created_datetime
    ,h_dn.dn_update_dt --updated_datetime
    ,h_dn.min_create_by --created_by
    ,h_dn.dn_updated_by --updated_by
    ,h_dn.ship_to --ship_to_address
    ,h_dn.real_ship_to_address --real_shipto_address
    ,h_dn.planned_gi_date --planned_gi_date
    ,h_dn.actual_gi_date --actual_gi_date
    ,cust_receipt.receiving_confirmation_date
    ,h_dn.delivery_mode --delivery_mode
    ,h_dn.carrier --carrier_id
    ,h_dn.pick_location --pick_location_id
    ,h_dn.total_qty
	,h_dn.plant
    ,date_format(h_dn.min_create_dt,'yyyy-MM-dd')
from
(
    select 
        so_no
       ,sap_delivery_no
       ,min(dn_create_dt) as min_create_dt
       ,max(dn_update_dt) as dn_update_dt
       ,min(dn_create_by) as min_create_by
       ,max(dn_updated_by) as dn_updated_by
       ,ship_to
       ,real_ship_to_address
       ,max(planned_gi_date) as planned_gi_date
       ,max(actual_gi_date) as actual_gi_date
       ,delivery_mode
       ,carrier
       ,pick_location
	   ,plant
	   ,sum(case when trim(qr_code) <> ''  and qr_code is not null then 1 else qty end) as total_qty
    from opsdw.ods_sales_delivery
    where dt='2022-05-12'
	and dn_create_dt is not null 
    group by so_no
            ,sap_delivery_no
			,ship_to
            ,real_ship_to_address
			,delivery_mode
            ,carrier
            ,pick_location
			,plant
)h_dn
left outer join 
(
    select delivery_no
        ,max(last_confirmation_date) as receiving_confirmation_date
    from opsdw.ods_so_dn_receiving_confirmation
    where last_confirmation_date is not null
	group by delivery_no )cust_receipt on h_dn.sap_delivery_no=cust_receipt.delivery_no
;
"
# Sales order delivery note lines
lines_sql="
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

insert overwrite table opsdw.dwd_fact_sales_order_dn_detail partition(dt)
select distinct
    l_dn.so_no
    ,l_dn.sap_delivery_no --delivery_id
    ,l_dn.delivery_line --line_number
    ,l_dn.material
    ,l_dn.qty
    ,l_dn.batch
    ,l_dn.qr_code
	,l_dn.plant
    ,l_dn.dt_partition  --yong dn de 创建日期分区
from
(
    select so_no 
        ,sap_delivery_no
        ,delivery_line
        ,material 
        ,case when trim(qr_code) <> '' and qr_code is not null then 1 else qty end as qty 
        ,qr_code 
        ,batch
        ,min(date_format(dn_create_dt, 'yyyy-MM-dd')) as dt_partition
		,plant
    from opsdw.ods_sales_delivery
    where dt='2022-05-12'
    and qty !=0
  group by so_no 
        ,sap_delivery_no
        ,delivery_line
        ,material 
        ,case when trim(qr_code) <> '' and qr_code is not null then 1 else qty end
        ,qr_code 
        ,batch
		,plant
)l_dn;
"

# 2. 执行加载数据SQL
if [ "$1"x = "so_dn"x ];then
	echo "$1 only run"
	$hive -e "$dn_header_sql"
	$hive -e "$lines_sql" 
else
    echo "wo_qrcode!  le_srr all run"

fi   


echo "End syncing so dn data into DWD layer on ${sync_date} .................." 
