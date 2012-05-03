#!/bin/sh
git pull;mvn clean package;
hadoop jar "target/`ls target/ |grep jar`" com.happyelements.hive.web.Starter /home/hadoop/hehwi /home/hadoop/hehwi 9888 /main.html
