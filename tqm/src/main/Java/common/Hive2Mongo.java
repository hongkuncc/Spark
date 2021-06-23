package common;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import entity.Hive2MongoConfigInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @ClassName: Hive2Mongo
 * @Description: 匹配汇总工具类
 * @Author: hongkuncc
 * @Date 2021/6/17 15:06
 * @Version 1.0
 */
public class Hive2Mongo {
    /*
     * @Author hongkuncc
     * @Description
     * @Date  2021/6/17 15:06
     * @Param
     * @Param
     * @Return replacement 是否以替换文档的形式更新Mongo中的文档"true","false"
     * @MethodName
     */
    private static void hive2Mongo(Hive2MongoConfigInfo hive2MongoConfig, String executeSql, String mongoTargetCollection, String replaceDocument){
        //配置参数
        SparkSession sparkSession = hive2MongoConfig.getSparkSession();
        JavaSparkContext jsc = hive2MongoConfig.getJsc();
        Logger log = hive2MongoConfig.getLog();
        Map<String,String> writeOverWrite =new HashMap<String, String>(16);
        Dataset<Row> executeSqlDataFrame = sparkSession.sql(executeSql);

        writeOverWrite.put("collection",mongoTargetCollection);
        writeOverWrite.put("replacement",replaceDocument);
        writeOverWrite.put("ordered","false");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverWrite);

        log.info("==========导入" + mongoTargetCollection + "开始");
        MongoSpark.save(executeSqlDataFrame,writeConfig);
        log.info("==========导入" + mongoTargetCollection + "结束");

    }

    public static void hive2Mongo(Hive2MongoConfigInfo hive2MongoConfig,String executeSql,String mongoTargetCollection ) {
        hive2Mongo(hive2MongoConfig,executeSql,mongoTargetCollection,"true");
    }
    public static void hive2MongoFalse(Hive2MongoConfigInfo hive2MongoConfig,String executeSql,String mongoTargetCollection ) {
        hive2Mongo(hive2MongoConfig,executeSql,mongoTargetCollection,"false");
    }

}
