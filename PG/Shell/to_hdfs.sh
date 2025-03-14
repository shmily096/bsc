
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
                                            FROM TRANS_SalesDelivery
                                            where format(DNCreateDT, 'yyyy-MM-dd')BETWEEN'2021-01-01'and '2021-04-31'"
											}
# 按业务分类同步数据
if [ "$1"x = "so_dn"x ];then
	echo "$1 only run"
	sync_sales_order_dn 
else
    echo "wo_qrcode!  le_srr all run"

fi    
