#!/bin/sh
git pull;mvn clean package;
hehwi=/home/hadoop/hehwi
cp -f $hehwi/target/`ls $hehwi/target |grep jar` /home/hadoop/hive/lib/hive.web.jar
netstat -ntpl |grep 9999 | awk '{print $NF}' |awk -F"/" '{print $1}' | xargs kill -9
sh /data/scripts/alive.sh

