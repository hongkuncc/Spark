package job;

import common.InitSparkSession;
import entity.Hive2MongoConfigInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @ClassName: Hive2Mongo
 * @Description: 解析xml写入
 * @Author: hongkuncc
 * @Date 2021/6/21 16:06
 * @Version 1.0
 */
public class Hive2MongoFromControl extends InitSparkSession {
    private static final Logger log = LoggerFactory.getLogger(Hive2MongoFromControl.class);

    public static void main(String[] args) {
    }

    public static void hive2Mongo(JavaSparkContext jsc, Map<String,String> xmlMap, Hive2MongoConfigInfo hive2MongoConfigInfo) {
        if (jsc == null) {
            
        }
        
    }
}
