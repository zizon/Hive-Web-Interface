#!/bin/sh
git pull;mvn clean package;
netstat -ntpl |grep 9999 | awk '{print $NF}' |awk -F"/" '{print $1}' | xargs kill -9
sh /data/scripts/alive.sh
