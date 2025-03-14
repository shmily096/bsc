#!/bin/bash

if [ "$1"x = "demo"x ];then
	echo "name is ok $1"
	
    
else
    echo "plesase use wo_qrcode!"
    
fi

if [ -n "$2" ] ;then 
    echo "name is ok $2"
else
   echo 'please input 第二个变量 the PostgreSQL db to be synced!'
   exit 1
fi