#!/bin/bash
sudo apt-get install python-numpy python-scipy python-matplotlib
cd $HADOOP_HOME/sbin
./start-dfs.sh
cd ~/
cd spark/sbin
./start-all.sh
cd ~/DTWspark
sbt package
hadoop fs -rm /simple-project_2.10-1.0.jar
hadoop fs -put target/scala-2.10/simple-project_2.10-1.0.jar /

