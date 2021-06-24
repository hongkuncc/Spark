#!/bin/sh
source ExitCodeCheck.sh
exitCodeCheck(){
if [ $1 -ne 0 ]; then
  echo 'shell execute return value is' $1 'is not 0'
  exit $1
else
  echo 'shell execute success'
fi
}
opts=$@


getparam(){
arg=$1
echo $opts |xargs -n1 |cut -b 2- |awk -F'=' '{if($1=="'"$arg"'") print $2}'
}


IncStart=`getparam inc_start`
IncEnd=`getparam inc_end`
oracle_connection=`getparam jdbc_str`
oracle_username=`getparam db_user`
oracle_password=`getparam db_psw`
dataName=`getparam db_sid`
queueName=`getparam hdp_queue`
hdfshostname=`getparam hdfs_host`;


IncStartYear=`echo ${IncStart:0:4}`;
IncStartMonth=`echo ${IncStart:4:2}`;
IncStartDay=`echo ${IncStart:6:2}`;
IncStartAll=${IncStartYear}"-"${IncStartMonth}"-"${IncStartDay}" 00:00:00.0";
IncStartAllFormat=${IncStartYear}"-"${IncStartMonth}"-"${IncStartDay};
IncEndYear=`echo ${IncEnd:0:4}`;
IncEndMonth=`echo ${IncEnd:4:2}`;
IncEndDay=`echo ${IncEnd:6:2}`;
IncEndAll=${IncEndYear}"-"${IncEndMonth}"-"${IncEndDay}" 00:00:00.0";
IncEndAllFormat=${IncEndYear}"-"${IncEndMonth}"-"${IncEndDay};


OneDayAgo=`date -d "$IncStart 1 days ago  " +%Y%m%d  `;
OneDayAgoYear=`echo ${OneDayAgo:0:4}`;
OneDayAgoMonth=`echo ${OneDayAgo:4:2}`;
OneDayAgoDay=`echo ${OneDayAgo:6:2}`;
OneDayAgoAll=${OneDayAgoYear}"-"${OneDayAgoMonth}"-"${OneDayAgoDay}" 00:00:00.0";
OneDayAgoAllFormat=${OneDayAgoYear}"-"${OneDayAgoMonth}"-"${OneDayAgoDay};

#任务名取脚本名
job_name=$0

rm -rf ${job_name}.jar
rm -rf ${job_name}
mkdir ${job_name}

hadoop fs -get /apps/hduser1594/sx_lidrp_safe/lcloud-lidrp-dap/jar/${job_name}.jar
hadoop fs -get /apps/hduser1594/sx_lidrp_safe/lcloud-lidrp-dap/jar/${job_name}.jar
hadoop fs -get /apps/hduser1594/sx_lidrp_safe/lcloud-lidrp-dap/jar/${job_name}.jar
hadoop fs -get /apps/hduser1594/sx_lidrp_safe/lcloud-lidrp-dap/jar/${job_name}.jar


SparkTaskName='spark_cgi_pssp_group_info_education';
SparkExecutorMemory='1G';
SparkExecutorCores='1';
SparkJar='spark2phoenix.jar';
SparkDriverMemory='1G';


queueName='queue_0101_01'
appName='spark_cgi_pssp_group_info_education' ;
sql='SELECT id_icm,ecif_no,agent_code, phone_number FROM an_pafc_safe.idld_lcdm_mit_client_group_info WHERE GROUP_CODE='050108' limit 1000000';
outputTable='cgi.pssp_group_info_education';




#=========开发参数===========
#kafka_metadata_broker_list="10.20.24.151:9092,10.20.24.159:9092,10.20.24.161:9092"    
#zookeeper_quorum="10.20.24.151:2181,10.20.24.159:2181,10.20.24.161:2181"  
#phoenix_jdbc_url="10.20.24.151,10.20.24.159,10.20.24.161:2181:/gbd2-hbase-kylin"  
#
#=========测试参数===========
#kafka_metadata_broker_list="30.4.64.78:9092,30.4.64.76:9092,30.4.64.77:9092"    
#zookeeper_quorum="30.4.64.78:2181,30.4.64.77:2181,30.4.64.76:2181"  
#phoenix_jdbc_url="30.4.64.78,30.4.64.77,30.4.64.76:2181:/gbd2-hbase-kylin"  


#=========生产参数===========
kafka_metadata_broker_list="30.4.32.71:9092,30.4.32.72:9092,30.4.32.73:9092"
zookeeper_quorum="30.4.32.71:2181,30.4.32.72:2181,30.4.32.73:2181"
phoenix_jdbc_url="30.16.16.29,30.16.16.33,30.16.16.30:2181:/gbd2-hbase02"


#使用spark版本
export SPARK_HOME=/appcom/spark-2.2.1
export SPARK_CONF_DIR=/appcom/spark-2.2.1-config


/appcom/spark-2.2.1/bin/spark-submit  \
--class hongkuncc.job.Hive2MongoFromControl \
--name "${job_name}"   \
--jars ./${job_name}/dom4j-1.6.1.jar  \
--master yarn-client \
--queue ${queueName} \
--files ./mongo.properties ./kafka.properties \
--num-executors 10 \
--executor-memory 15G \
--executor-cores 4 \
--driver-memory 10G \
--conf spark.driver.maxResultSize=3G \
--conf spark.sql.autoBroadcastJoinThreshold=20971520 \
--conf spark.default.parallelism=1200 \
--conf spark.sql.shuffle.partitions=1200 \
--conf spark.kryoserializer.buffer.max=300m \
--conf spark.shuffle.io.maxRetries=60 \
--conf spark.shuffle.io.retryWait=120s \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.yarn.driver.memoryOverhead=2048 \
--conf spark.core.connection.ack.wait.timeout=600 \
--conf spark.rpc.message.maxSize=100 \
--conf spark.shuffle.manager=SORT \
--conf spark.network.timeout=600s \
--conf spark.sql.codegen=true \
--conf spark.sql.inMemoryColumnStorage.compressed=true \
--conf spark.port.maxRetries=1000 \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+MaxPermSize=64m -XX:+UseG1GC -XX:NewRatio=4" \
--conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
#--conf spark.executor.extraJavaOptions="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC #-XX:+CMSParallelRemarkEnabled -XX:+ParallelRefProcEnabled -XX:+CMSClassUnloadingEnabled #-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC #-XX:+HeapDumpOnOutOfMemoryError -verbose:gc " \
${SparkJar} "${appName}" "${sql}" "${outputTable}" "${phoenix_jdbc_url}"



#1 spark.sql.codegen 默认值为false，当它设置为true时，Spark SQL会把每条查询的语句在运行时编译为java的二进制代码。这有什么作用呢？它可以提高大型查询的性能，但是如果进行小规模的查询的时候反而会变慢，就是说直接用查询反而比将它编译成为java的二进制代码快。所以在优化这个选项的时候要视情况而定。
#
#2 spark.sql.inMemoryColumnStorage.compressed 默认值为false 它的作用是自动对内存中的列式存储进行压缩
#
#3 spark.sql.inMemoryColumnStorage.batchSize 默认值为1000 这个参数代表的是列式缓存时的每个批处理的大小。如果将这个值调大可能会导致内存不够的异常，所以在设置这个的参数的时候得注意你的内存大小
#
#4 spark.sql.parquet.compressed.codec 默认值为snappy 这个参数代表使用哪种压缩编码器。可选的选项包括uncompressed/snappy/gzip/lzo
#
#uncompressed这个顾名思义就是不用压缩的意思


exitCodeCheck $?

