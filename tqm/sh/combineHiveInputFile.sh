#!/bin/bash
export HADOOP_CLIENT_OPTS="Xmx20480m"
hive -e "
set mapred.job.queue.name=root.queue_sx80.1594_02;
set mapred.job.name=movedata2partition;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=256000000;
set mapred.max.split.size.per.node=100*1000*1000;
set mapred.max.split.size.per.rack=100*1000*1000;
set mapreduce.map.memory.mb=8092;
set mapreduce.map.java.opts=-Xmx3276m;
set mapreduce.reduce.memory.mb=8092;
set mapreduce.reduce.java.opts=-Xmx3276m;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

--sql
"

