#!/bin/bash
#定时重启释放资源,适用于58，59hive
hiveservices.sh stop
sleep 10
for i in {1..10}
do
	#取第3行
	a=`hiveservices.sh status | sed -n 3p`
	if [ "${a}" = "HiveServer2 服务运" ]; then
		echo "Thriftserver 服务运行正常"		
		break 
	else
		hiveservices.sh start 
		echo "第 $i 次"
		sleep 40
	fi
done
