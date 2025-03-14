#!/bin/bash
# Function:
#   initialize the BSC BI database
# History:
#   2021-05-11    Donny   v1.0    draft

hive=/opt/module/hive3/bin/hive  # Hive的配置路径

if [ -n "$1" ] ;then 
    default_db=$1
else
    default_db='opsdw'
fi

hdfs dfs -mkdir -p /bsc/${default_db}

sql="create database if not exists $default_db location '/bsc/${default_db}';"

# 2. 加载数据
$hive -e "$sql"