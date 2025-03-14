#!/bin/bash
# Function:
#   sync up dws_dsr_daily_trans 
# History:
# 2021-06-29    Donny   v1.0    init
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
if [ -n "$2" ] ;then 
    last_Q=$2
else
    last_Q=`date -d '3 month ago' +%Y-%m-%d`
fi
dws_fact_inventory_onhand_by_mon_maxdt=`hdfs dfs -ls /bsc/opsdw/dws/dws_fact_inventory_onhand_by_mon | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`
echo "start syncing dws_dsr_daily_trans data into DWS layer on   .................."
so_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dws_finance_upn_qty_so;
create table tmp_dws_finance_upn_qty_so stored as orc as
select 
	COALESCE(b.excel,a.division_id,'Others')  as bu,
	COALESCE(a.division_id,'Others') as disvsion_nm,
	material ,
	so_no,
    order_type,
	sum(qty)as qty,
    count(distinct so_no)as po_count,
    sum(net_value) as net_value,
	date(chinese_socreatedt ) as dt
from (select * from dwd_fact_sales_order_info 
                where dt>=(DATE_ADD(trunc('$last_Q','MM'),-1)) 
                and date(chinese_socreatedt ) >=trunc('$last_Q','MM')) a 
left join dwd_dim_finance_disvision_mapping_hive_disvsion b on a.division_id =b.hive 
group by 
	COALESCE(b.excel,a.division_id,'Others'),
	COALESCE(a.division_id,'Others') ,
	material,
	so_no,
    order_type,
	date(chinese_socreatedt );
--------------------------------------------------------------------------po count
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select 
    bu,
    disvsion_nm,
    material ,
    so_no AS order_no,
    '' as delivery_id,
    order_type,
    po_count AS value,
    dt,
    'po count' as category,
    date_format(dt,'yyyyMM')as year_mon
from tmp_dws_finance_upn_qty_so;
--------------------------------------------------------------------------po qty
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select 
    bu,
    disvsion_nm,
    material ,
    so_no AS order_no,
    '' as delivery_id,
    order_type,
    qty AS value,
    dt,
    'po qty' as category,
    date_format(dt,'yyyyMM')as year_mon
from tmp_dws_finance_upn_qty_so;
--------------------------------------------------------------------------po amount
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select 
    bu,
    disvsion_nm,
    material ,
    so_no AS order_no,
    '' as delivery_id,
    order_type,
    net_value AS value,
    dt,
    'po amount' as category,
    date_format(dt,'yyyyMM')as year_mon
from tmp_dws_finance_upn_qty_so;
"
delivery_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------Outbound Unit qty
drop table if exists tmp_dws_finance_upn_qty_delivery;
create table tmp_dws_finance_upn_qty_delivery stored as orc as
    SELECT 
        COALESCE(b.excel,ddm.division_display_name,'Others')as bu,
        COALESCE(ddm.division_display_name,'Others') as disvsion_nm,
        a.material,
        a.so_no,
        delivery_id,
        order_type,
        sum(qty) as qty,
        'outbound_unit_qty' as category,  
        date(chinese_dncreatedt ) as dt        
    From (select * from dwd_fact_sales_order_dn_detail 
                    where dt>=(DATE_ADD(trunc('$last_Q','MM'),-1))
                    and date(chinese_dncreatedt ) >=trunc('$last_Q','MM'))a
    left join (select  material_code,division_display_name from dwd_dim_material where dt='$dwd_dim_material_maxdt')ddm 
        on a.material =ddm.material_code 
    left join dwd_dim_finance_disvision_mapping_hive_disvsion b 
        on ddm.division_display_name =b.hive 
    left join (select distinct so_no  ,order_type from dwd_fact_sales_order_info where dt>add_months(trunc('$last_Q','MM'),-6) ) ss 
        on a.so_no =ss.so_no     
    group by 
        COALESCE(b.excel,ddm.division_display_name,'Others'),
        COALESCE(ddm.division_display_name,'Others') ,
        a.material,
        a.so_no,
        delivery_id,
        order_type,
        date(chinese_dncreatedt );
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select
    bu,
    disvsion_nm,
    material ,
    so_no as order_no,
    delivery_id,
    order_type,
    qty as value,
    dt,
    category,
    date_format(dt,'yyyyMM')as year_mon
from tmp_dws_finance_upn_qty_delivery
"
inbound_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------Inbound Unit
drop table if exists tmp_dws_finance_upn_qty_inbound;
create table tmp_dws_finance_upn_qty_inbound stored as orc as
SELECT 
        COALESCE(b.excel,ddm.division_display_name,'Others')as bu,
        COALESCE(ddm.division_display_name,'Others') as disvsion_nm,
         a.material_code ,
        a.sto_no ,
        a.delivery_no ,
        sum(a.qty)as qty,
        'inbound_unit_qty' as category,
        date(a.chinese_create_dt )as dt        
from (select * from opsdw.dwd_fact_import_export_dn_detail 
            where dt>=(DATE_ADD(trunc('$last_Q','MM'),-1))
            and date(chinese_create_dt ) >=trunc('$last_Q','MM')
             and actual_migo_dt is not null)a
left join (select  material_code,division_display_name from dwd_dim_material where dt='$dwd_dim_material_maxdt')ddm 
    on a.material_code =ddm.material_code 
left join dwd_dim_finance_disvision_mapping_hive_disvsion b on ddm.division_display_name =b.hive 
group by 
        COALESCE(b.excel,ddm.division_display_name,'Others'),
        COALESCE(ddm.division_display_name,'Others') ,
        a.sto_no ,
        a.delivery_no ,
        a.material_code ,
        date(a.chinese_create_dt );
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select
    bu,
    disvsion_nm,
    material_code as material ,
    sto_no as order_no,
    delivery_no as delivery_id,
    '' as order_type,
    qty as value,
    dt,
    category,
    date_format(dt,'yyyyMM')as year_mon    
from tmp_dws_finance_upn_qty_inbound
"
onhand_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------onhand
drop table if exists tmp_dws_finance_upn_qty_onhand;
create table tmp_dws_finance_upn_qty_onhand stored as orc as
SELECT 
    COALESCE(b.excel,ddm.division_display_name,'Others')as bu,
    COALESCE(ddm.division_display_name,'Others') as disvsion_nm,    
    material,
    plant,
    SUM(quantity)qty,
    case when unrestricted>0 then 'unrestricted'
        when inspection>0 then 'inspection'
        when blocked_material>0 then 'blocked_material'
        else 'others'
    end as order_type,
    'onhand_'||inventory_type as category,
    year_mon ,
    update_date as dt    
from (select * from dws_fact_inventory_onhand_by_mon where year_mon='$dws_fact_inventory_onhand_by_mon_maxdt' and (inventory_type='DC' or inventory_type='FIELD') AND plant IN ('D835','D836','D837','D838')) a
left join (select  material_code,division_display_name from dwd_dim_material where dt='$dwd_dim_material_maxdt')ddm 
    on a.material =ddm.material_code 
left join dwd_dim_finance_disvision_mapping_hive_disvsion b on ddm.division_display_name =b.hive 
GROUP BY COALESCE(b.excel,ddm.division_display_name,'Others'),
        COALESCE(ddm.division_display_name,'Others') ,
        year_mon ,
        inventory_type,
        material,
        plant,
        case when unrestricted>0 then 'unrestricted'
            when inspection>0 then 'inspection'
            when blocked_material>0 then 'blocked_material'
            else 'others'
        end,
        update_date;
INSERT OVERWRITE TABLE ${target_db_name}.dws_finance_upn_qty partition(category,year_mon)
select 
    bu,
    disvsion_nm,
    material ,
    plant as order_no,
    '' as delivery_id,
    order_type,
    qty as value,
    date(dt) as dt,
    category,
    replace(year_mon ,'-','') as year_mon    
from tmp_dws_finance_upn_qty_onhand
"
# 2. 执行加载数据SQL
# 记录脚本开始执行的时间  
start_time=$(date +%s)  
  
# 在这里写下你的脚本逻辑  
if [ "$1"x = "so"x ];then
	echo "dws $1 only run"	
	$hive -e "$so_sql"
elif [ "$1"x = "delivery"x ];then
	echo "dws $1 only run"	
	$hive -e "$delivery_sql"
elif [ "$1"x = "inbound"x ];then
	echo "dws $1 only run"	
	$hive -e "$inbound_sql"
elif [ "$1"x = "onhand"x ];then
	echo "dws $1 only run"	
	$hive -e "$onhand_sql"
else
    echo "--------------------------------------------------------------------------po count,qty,amount"
    $hive -e "$so_sql"
    echo "--------------------------------------------------------------------------delivery"
    $hive -e "$delivery_sql"
    echo "--------------------------------------------------------------------------inbound"
    $hive -e "$inbound_sql"
    echo "--------------------------------------------------------------------------onhand"
    $hive -e "$onhand_sql"
fi
# 记录脚本结束执行的时间  
end_time=$(date +%s)  
  
# 计算脚本运行时间（以秒为单位）  
execution_time=$((end_time - start_time))  
  
# 将秒转换为分钟和秒的形式  
minutes=$((execution_time / 60))  
seconds=$((execution_time % 60))  
  
echo "脚本运行时间：$minutes 分钟 $seconds 秒"
#导入pg
sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dws_finance_upn_qty
sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dws_finance_upn_qty


echo "End syncing dws_finance_upn_qty data into DWS layer on  .................."