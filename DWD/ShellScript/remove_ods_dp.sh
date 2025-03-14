#!/bin/bash
echo "Reomve the data a week ago data"
begin_day=-37
end_day=-30
for((i="$begin_day";i<="$end_day";i++))
do
    remove_day=`date -d "$i days" +%F`
	echo "Tobe Removed: ${remove_day}"
	DATA_path="/bsc/opsdw/ods/ods_kpi_dpchinadatabase_data/dt=${remove_day}"
	KPI_path="/bsc/opsdw/ods/ods_kpi_dpchinadatabase_kpi/dt=${remove_day}"
	ods_customer_master="/bsc/opsdw/ods/ods_customer_master/dt=${remove_day}"
	hdfs dfs -test -d $DATA_path
	if [ $? -eq 0 ]
	then
		echo "Start remove $DATA_path"
		hdfs dfs -rm -f -r $DATA_path
		echo "end remove $DATA_path"
	fi
	hdfs dfs -test -d $KPI_path
	if [ $? -eq 0 ]
	then
		echo "Start remove $KPI_path"
		hdfs dfs -rm -f -r $KPI_path
		echo "end remove $KPI_path"
	fi
	hdfs dfs -test -d $ods_customer_master
	if [ $? -eq 0 ]
	then
		echo "Start remove $ods_customer_master"
		hdfs dfs -rm -f -r $ods_customer_master
		echo "end remove $ods_customer_master"
	fi
done
