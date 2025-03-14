#!/bin/bash
#把财务的几个excle,eeo,辅料采购明细导入pg然后导入hive计算。还有每月一号清空下tmp文件夹
if [ -n "$1" ]; then
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
sqoop="/opt/module/sqoop/bin/sqoop"
pg_host='10.226.98.58'
pg_port='55433'
pg_user='postgres'
pg_password='1qazxsw2'
pg_dbname='tableaudb'
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_cs="	select count(1)  from public.finance_monthly_opex_cs WHERE date(updatetime)='$sync_date'"
result_cs=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_cs}")
value_cs=$(echo $result_cs | awk 'END {a_cs=$(NF-2)} END {print a_cs}')
if [[ $value_cs -eq 0 ]]; then
    echo "pg cs date is count is 0 then not run。"
else
    echo "$value_cs pg date not 0 then run"
	echo "--------------------------------------start read finance_monthly_opex_cs"
	sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_monthly_opex_cs
	echo "--------------------------------------start load to dws_finance_monthly_cs_freight_ocgs_htm"
	sh /bscflow/dws/dwd_to_dws_finance_monthly_cs_freight_ocgs_htm.sh cs
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_fre="	select count(1)  from public.finance_monthly_opex_freight WHERE date(updatetime)='$sync_date'"
result_fre=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_fre}")
value_fre=$(echo $result_fre | awk 'END {a_fre=$(NF-2)} END {print a_fre}')
if [[ $value_fre -eq 0 ]]; then
    echo "pg fre date is count is 0 then not run。"
else
    echo "$value_fre pg date not 0 then run"
	echo "--------------------------------------start read finance_monthly_opex_freight"
	sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_monthly_opex_freight
	echo "--------------------------------------start load to dws_finance_monthly_cs_freight_ocgs_htm"
	sh /bscflow/dws/dwd_to_dws_finance_monthly_cs_freight_ocgs_htm.sh fre
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_ocgs=" select count(1)  from public.finance_monthly_opex_ocogs WHERE date(updatetime)='$sync_date'"
result_ocgs=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_ocgs}")
value_ocgs=$(echo $result_ocgs | awk 'END {a_ocgs=$(NF-2)} END {print a_ocgs}')
if [[ $value_ocgs -eq 0 ]]; then
    echo "pg ocgs date is count is 0 then not run。"
else
    echo "$value_ocgs pg date not 0 then run"
	echo "--------------------------------------start read finance_monthly_opex_ocogs"
	sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_monthly_opex_ocogs
	echo "--------------------------------------start load to dws_finance_monthly_cs_freight_ocgs_htm"
	sh /bscflow/dws/dwd_to_dws_finance_monthly_cs_freight_ocgs_htm.sh ocgs
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_hfm="select count(1)  from public.finance_ocogs_report_china_hfm WHERE date(updatetime)='$sync_date' "
result_hfm=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_hfm}")
value_hfm=$(echo $result_hfm | awk 'END {a_hfm=$(NF-2)} END {print a_hfm}')
if [[ $value_hfm -eq 0 ]]; then
    echo "pg hfm date is count is 0 then not run。"
else
    echo "$value_hfm pg date not 0 then run"
	echo "--------------------------------------start read finance_ocogs_report_china_hfm"
	sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_ocogs_report_china_hfm
	echo "--------------------------------------start load to dws_finance_monthly_cs_freight_ocgs_htm"
	sh /bscflow/dws/dwd_to_dws_finance_monthly_cs_freight_ocgs_htm.sh htm
fi
# 验证PG数据库连接并执行查询语句，如果pg以下4个表任一有今天的数据，就跑dws到dwt做数据汇总更新，最后导入pg
query_dwt="SELECT count(1) FROM (select date(max(dt))dt,sum(ct)as ct from (
	select max(updatetime)dt,count(1)ct  from public.finance_monthly_opex_cs
	union all
	select max(updatetime)dt,count(1)ct  from public.finance_monthly_opex_freight
	union all
	select max(updatetime)dt,count(1)ct  from public.finance_monthly_opex_ocogs
	union all
	select max(updatetime)dt,count(1)ct  from public.finance_ocogs_report_china_hfm)x)a  WHERE dt='$sync_date'
    "
result_dwt=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_dwt}")
value_dwt=$(echo $result_dwt | awk 'END {a_dwt=$(NF-2)} END {print a_dwt}')
if [[ $value_dwt -eq 0 ]]; then
    echo "pg all date is count is 0 then not run。"
else
    echo "$value_dwt pg date not 0 then run"
	echo "--------------------------------------start dws to dwt"
	sh /bscflow/dwt/dws_to_dwt_finance_template_cs_freight_ocgs_htm.sh
    echo "--------------------------------------start dwt to hdfs"
    sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_finance_monthly_cs_freight_ocgs_htm
    echo "--------------------------------------start hdfs to 58pg"
    sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_finance_monthly_cs_freight_ocgs_htm
fi
#月初 
m_s_date=${sync_date:0:7}-01
#每月1号清空tmp
if [ "$sync_date"x = "$m_s_date"x ];then
	echo "this day is 01 rm -rf /tmp/*"	
	rm -rf /tmp/*
else
	echo "not delete"
fi
###########################################################################EEO
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_excess="select count(1)  from public.finance_excess_reserve_detail WHERE date(updatedt)='$sync_date' "
result_excess=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_excess}")
value_excess=$(echo $result_excess | awk 'END {a_excess=$(NF-2)} END {print a_excess}')
if [[ $value_excess -eq 0 ]]; then
    echo "pg excess date is count is 0 then not run。"
else
    echo "$value_excess pg date not 0 then run"
    echo "--------------------------------------start read finance_excess_reserve_detail"
    sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_excess_reserve_detail
    echo "--------------------------------------start Write to dws_eeo_detail"
    sh /bscflow/dws/dwd_to_dws_eeo_detail.sh Excess
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_expired="select count(1)  from public.finance_expired_reserve_detail WHERE date(updatedt)='$sync_date' "
result_expired=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_expired}")
value_expired=$(echo $result_expired | awk 'END {a_expired=$(NF-2)} END {print a_expired}')
if [[ $value_expired -eq 0 ]]; then
    echo "pg expired date is count is 0 then not run。"
else
    echo "$value_expired pg date not 0 then run"
    echo "--------------------------------------start read finance_expired_reserve_detail"
    sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_expired_reserve_detail
    echo "--------------------------------------start Write to dws_eeo_detail"
    sh /bscflow/dws/dwd_to_dws_eeo_detail.sh expired
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_obsolete="select count(1)  from public.finance_obsolete_reserve_detail WHERE date(updatedt)='$sync_date' "
result_obsolete=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_obsolete}")
value_obsolete=$(echo $result_obsolete | awk 'END {a_obsolete=$(NF-2)} END {print a_obsolete}')
if [[ $value_obsolete -eq 0 ]]; then
    echo "pg obsolete date is count is 0 then not run。"
else
    echo "$value_obsolete pg date not 0 then run"
    echo "--------------------------------------start read finance_obsolete_reserve_detail"
    sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_obsolete_reserve_detail
    echo "--------------------------------------start Write to dws_eeo_detail"
    sh /bscflow/dws/dwd_to_dws_eeo_detail.sh obsolete
fi
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_reval="select count(1)  from public.finance_std_reval_detail WHERE date(updatedt)='$sync_date' "
result_reval=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_reval}")
value_reval=$(echo $result_reval | awk 'END {a_reval=$(NF-2)} END {print a_reval}')
if [[ $value_reval -eq 0 ]]; then
    echo "pg reval date is count is 0 then not run。"
else
    echo "$value_reval pg date not 0 then run"
    echo "--------------------------------------start read finance_std_reval_detail"
    sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_std_reval_detail
fi
# 验证PG数据库连接并执行查询语句，如果pg以下4个表任一有今天的数据，就跑detail到dws_eeo做数据汇总更新
query_eeo="SELECT count(1) FROM (select date(max(dt))dt,sum(ct)as ct from (
	select max(updatedt)dt,count(1)ct  from public.finance_excess_reserve_detail
	union all
	select max(updatedt)dt,count(1)ct  from public.finance_expired_reserve_detail
	union all
	select max(updatedt)dt,count(1)ct  from public.finance_obsolete_reserve_detail
	union all
	select max(updatedt)dt,count(1)ct  from public.finance_std_reval_detail)x)a  WHERE dt='$sync_date'
    "
result_eeo=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_eeo}")
value_eeo=$(echo $result_eeo | awk 'END {a_eeo=$(NF-2)} END {print a_eeo}')
if [[ $value_eeo -eq 0 ]]; then
    echo "pg all eeo date is count is 0 then not run。"
else
    echo "$value_eeo pg date not 0 then run"
	echo "--------------------------------------start detail to dws_eeo"
	sh /bscflow/dws/dwd_to_dws_eeo.sh
    # echo "--------------------------------------start dwt to hdfs"
    # sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_finance_monthly_cs_freight_ocgs_htm
    # echo "--------------------------------------start hdfs to 58pg"
    # sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_finance_monthly_cs_freight_ocgs_htm
fi
###############################################################辅料采购明细
# 验证PG数据库连接并执行查询语句如果pg有今天的数据就导入dwd存历史数据，dws存最新数据
query_operation="select count(1)  from public.finance_operation_upn_cost WHERE date(updatedt)='$sync_date' "
result_operation=$(sqoop eval \
  --connect jdbc:postgresql://${pg_host}:${pg_port}/${pg_dbname} \
  --username ${pg_user} \
  --password ${pg_password} \
  --query "${query_operation}")
value_operation=$(echo $result_operation | awk 'END {a_operation=$(NF-2)} END {print a_operation}')
if [[ $value_operation -eq 0 ]]; then
    echo "pg operation date is count is 0 then not run。"
else
    echo "$value_operation pg date not 0 then run"
    echo "--------------------------------------start read finance_operation_upn_cost"
    sh /bscflow/dwd/pg_to_hdfst_to_ods_to_dwd.sh finance_operation_upn_cost
fi