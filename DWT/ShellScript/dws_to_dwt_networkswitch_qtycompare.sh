#!,bin,bash
# Function:
#   sync up dwt_dsr_clear_inventory data to dwt layer
# History:
# 2023-08-08    slc   v1.0    init
export LANG="en_US.UTF-8"
# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$2" ] ;then 
    sync_date=$2
else
    #默认取昨天的日期
    sync_date=$(date -d '-1 day' +%F)
fi
curren_date=$(date  +%F)
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | awk 'BEGIN {max = 0} {if ($6+0 > max+0) max=$6} END {print $8}' | grep -oP 20'[^ ]*'`

echo "start syncing onhand data into DWT layer on ${sync_date} .................."
dwt_sql="
use ${target_db_name};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.auto.convert.join=false;

drop table if exists tmp_dwt_networkswitch_qtycompare_onhand;
create table  tmp_dwt_networkswitch_qtycompare_onhand stored as orc as 
		SELECT 
			plant,
			storage_loc,
			material,
			---SUM(case when inventory_type='DC' AND ((plant ='D835' AND storage_loc='0001')OR  (plant IN('D838','D837') AND storage_loc='Y001')) then unrestricted else 0 end) AS ur,
			SUM(case when inventory_type='DC' THEN unrestricted ELSE 0 END) AS ur,
			sum(case when inventory_type='DC' THEN inspection ELSE 0 END) as qi,
			--sum(inspection) as qi,
			sum(case when inventory_type='ITR' AND plant IN('D835','D838','D837') and plant_from not in ('D835','D838','D837','D836')then quantity else 0 end ) as git,
			sum(case when inventory_type='ITR' AND plant_from ='D835' and plant ='D838' then quantity else 0 end ) in_transit
		from dwd_fact_inventory_onhand 
		where dt='$sync_date' 
			and inventory_type in('DC','ITR')
			and plant IN('D835','D838','D837')
		GROUP BY plant,storage_loc,material;
		
insert overwrite table dwt_networkswitch_qtycompare partition(dt='$curren_date' )
select 
	aa.plant
	,aa.storage_loc
	,aa.sloc
	,aa.default_location
	,aa.material
	,sku.division_id
	,sku.division_display_name
	,aa.qty
	,aa.category
from (
	select 
		plant,
		storage_loc,
		'' as sloc,
		'' as default_location,
		material,
		ur as qty,
		'ur' as category
	from tmp_dwt_networkswitch_qtycompare_onhand 
	union all
	select 
		plant,
		storage_loc,
		'' as sloc,
		'' as default_location,
		material,
		qi as qty,
		'qi' as category
	from tmp_dwt_networkswitch_qtycompare_onhand 
	union all
	select 
		plant,
		storage_loc,
		'' as sloc,
		'' as default_location,
		material,
		git as qty,
		'git' as category
	from tmp_dwt_networkswitch_qtycompare_onhand 
	union all
	select 
		plant,
		storage_loc,
		'' as sloc,
		'' as default_location,
		material,
		in_transit as qty,
		'in_transit' as category
	from tmp_dwt_networkswitch_qtycompare_onhand 
	union all
	select 
		plant_dn as plant
		,pick_location_id as  storage_loc
		,'' as sloc
		,'' as default_location
		, material
		,sum(cast(qty as int)) as qty
		,'open_dn_qty' as category
	from dws_opendn_dned_daily
	where dt_year=date_format('$sync_date','YYYY')
	and cast(dt_month as int) =month('$sync_date')
	and plant_dn IN('D835','D838','D837')
	group by material
			,plant_dn
			,pick_location_id
	union all
	SELECT 
		plant,
		case when plant ='D835' then '0001' 
			when plant IN('D838','D837') then 'Y001'
		end as storage_loc,
		sloc,
		default_location,	
		material,
		sum(qty) as qty,
		'openorder_qty' AS category
	from opsdw.dwd_fact_openordercn 
	where insertdate>= '$sync_date' AND dctp IN('OR','KB','FD','ZNC')
	AND plant in ('D835','D838','D837')
	GROUP BY plant,material,
			case when plant ='D835' then '0001' 
				when plant IN('D838','D837') then 'Y001'
			end,
			default_location,
			sloc
)aa
LEFT JOIN 
(
select distinct
         bu_sku.material_code 
        ,bu_sku.division_id
        ,bu_sku.division_display_name
        --,bu_sku.default_location
        --,bu_sku.sub_division
        --,bu_sku.sub_division_bak
        --,bu_sku.business_group
    from dwd_dim_material bu_sku --源表主表MDM_MaterialMaster 全量更新,次表MDM_DivisionMaster 全量更新
    where  bu_sku.dt ='$dwd_dim_material_maxdt'
)sku
on aa.material=sku.material_code
"

# 2. 执行加载数据SQL
if [ "$1"x = "dwt_networkswitch_qtycompare"x ];then
	echo "hdfs $1 only run"	
    echo "$dwt_sql"
	$hive -e "$dwt_sql"
	echo "hdfs  finish dwt_networkswitch_qtycompare data into hdfs layer on ${yester_day} .................."
else
	echo "$1 not found"
fi
echo "End syncing  data into DWT layer on $sync_year .................."