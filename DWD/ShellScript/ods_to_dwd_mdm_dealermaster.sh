
n:
#   sync up sales order invocie data from ods to dwd layer
# History:
# 2022-05-27    slc   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# 默认取当前时间的前一天 
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dwd_mdm_dealermaster data into DWD layer on ${sync_date} .................."

# 1 Hive SQL string
sto_sql="
-- 参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;

-- sync up SQL string
insert overwrite table ${target_db_name}.dwd_mdm_dealermaster partition(dt)
select 
	dealercode,
	dealername,
	dealertype, 
	parentsapid,
	parentdealername,
	parentdealertype, 
	status, 
	hospitalcode,
	dealeraddress, 
	isactiveindms, 
	dealermarkettype,
	dealermarkettypedesc, 
	dealernameen, 
	dealeraddrprovince,
	dealeraddrcity, 
	dealertypealt, 
	dt
from ${target_db_name}.ods_mdm_dealermaster  ---源表:MDM_DealerMaster全量
where dt='$sync_date' and dealertype is  not null 
"
# 2. 执行加载数据SQL
$hive -e "$sto_sql"

echo "End syncing dwd_mdm_dealermaster DWD layer on ${sync_date} .................."
