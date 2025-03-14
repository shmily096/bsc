#!bin/bash
# Function:
#   load hdfs wo qr to ODS transation table
# demo:
# load data inpath '/bsc/origin_data/bsc_app_ops/sales_order/2020-05-07' overwrite
# History:
# 2021-07-01    Donny   v1.0    init
# 2021-07-29    Donny   v1.1    add partition

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
origin_db_name='bsc_app_ops' #原始数据库
hive=/opt/module/hive3/bin/hive  # Hive的配置路径

sync_date=$(date  +%F)
start=$1
end=$2
con_sql_str=''

for ((i="$start";i<="$end";i++))
do
    if [ "$i" -eq "$start" ]; then
        sub_sql_str="load data local inpath '/bscflow/data/qr$i.csv' overwrite into table ${target_db_name}.ods_work_order_qr_code_mapping partition(dt='$sync_date');"
        echo $sub_sql_str
    else
        sub_sql_str="load data local inpath '/bscflow/data/qr$i.csv' into table ${target_db_name}.ods_work_order_qr_code_mapping partition(dt='$sync_date');"
        echo $sub_sql_str
    fi
    con_sql_str="$con_sql_str""$sub_sql_str"
done

# 2. 执行加载数据SQL
$hive -e "$con_sql_str"

