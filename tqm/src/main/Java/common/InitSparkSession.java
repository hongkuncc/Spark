package common;


import constant.MongoConnectionConstant;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import util.ConfPropertyUtil;
import util.PostUtil;

import java.io.Serializable;
import java.util.Map;


/**
*@ClassName: InitSparkSession
*@Description: 创建SparkJob的方法
*@Author: hongkuncc
*@Date 2021/6/17 20:54
*@Version 1.0
*
*/public class InitSparkSession implements Serializable {

    /*
     * @Author hongkuncc
     * @Description
     * @Date  2021/6/18 9:31
     * @Param
     * @Param
     * @Return
     * @MethodName
     */

    public SparkSession getSparkSession(String jobName,Class sparkJob,Boolean needMongodb){
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
                .set("spark.sql.inMemoryColumnarStorage.batchSize","20000")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.debug.maxToStringFields","1000")
                .set("spark.sql.parquet.writeLegacyFormat","true")
                .set("spark.sql.autoBroadcastJoinThreshold","5242880")
                .registerKryoClasses(new Class[]{InitSparkSession.class,sparkJob});

        if (needMongodb) {
            String mongodbUrl = getMongodbUrl();
            sparkConf.set("","")
                    .set("","");
        }


        SparkSession sparkSession=SparkSession.builder()
                                                .appName(jobName)
                                                .config(sparkConf)
                                                .enableHiveSupport()
                                                .getOrCreate();
        return sparkSession;
    }

    private String getMongodbUrl(){
        ConfPropertyUtil propertyUtil = new ConfPropertyUtil();

        Map<String,String> map = propertyUtil.loadJobProperties(MongoConnectionConstant.MONGODB_PROPERTIERS);
        Map<String,String> cyberarkMap = propertyUtil.loadJobProperties("cyberark.properties");

        String postUrl = cyberarkMap.get("");
        String postAppId = cyberarkMap.get("");
        String postSafe = cyberarkMap.get("");
        String folder = cyberarkMap.get("");
        String object = cyberarkMap.get("");
        String reason = cyberarkMap.get("");
        String appKey = cyberarkMap.get("");

        String password = PostUtil.getPasswordFromRemote(postUrl,postAppId,postSafe,folder,object,reason,appKey);
        String urlBefore = map.get("mongodb.post.url.before");
        String urlafter = map.get("mongodb.post.url.after");

        return urlBefore + password + urlafter;
    }


}
