#! /bin/bash
cd ~/
clear
rm in*
echo "Enter the number of points to be generated for a time series: "
read POINTS
echo "Enter the Query size in terms of number of points : "
read BUF
echo "Enter number of files to split into : "
read FIL
echo "Deleting the directory Data in HDFS if present from previous runs."
hadoop fs -rm -r /Data
echo "Generating Data"
python gen.py `expr $POINTS / 2`
echo "Finished Generating Data"
python spliter.py out in $POINTS $FIL $BUF
echo "The generated data is stored in a file 'out' and is split into files indexed in00...in10.. and so on"
hadoop fs -mkdir /Data
echo "Putting data into HDFS"
hadoop fs -put in* /Data/
echo "Enter IP address to run server file: "
read IP
python sparkmasterbsf.py $IP &
PID=$!
echo "Start spark program."
SPARK_JAR=./assembly/target/scala-2.10/spark-assembly-1.0.0-SNAPSHOT-hadoop2.4.0.jar 
HADOOP_CONF_DIR=/home/sparkuser/hadoop/etc/hadoop 
./spark/bin/spark-submit --master spark://$IP:7077  --class SimpleApp ~/DTWspark/target/scala-2.10/simple-project_2.10-1.0.jar Query.txt `expr $BUF` 0.05 `expr $POINTS / $FIL`

kill -9 $PID
