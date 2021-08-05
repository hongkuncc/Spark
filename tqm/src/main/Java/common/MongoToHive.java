package common;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.bson.Document;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * @ClassName: MongoToHive
 * @Description:
 * @Author: hongkuncc
 * @Date 2021/8/4 20:25
 * @Version 1.0
 */
public class MongoToHive {
    /*
     * @Author hongkuncc
     * @Description 根据mongo集合名称和entity实例获取数据
     * @Date  2021/8/4 20:30
     * @Param mongoCollection mongo集合
     * @Param entity 实例
     * @Param pipeline mongoz端执行的过滤条件
     * @Return
     * @MethodName
     */
    public static Dataset getMongoDataset(JavaSparkContext jsc, String mongoCollection, Class entity){
        ReadConfig readConfig = getReadConfig(jsc,mongoCollection);
        Dataset mongoData = MongoSpark.load(jsc,readConfig)
                .toDS(entity);
        return mongoData;

    }

    public static Dataset getMongoDataset(JavaSparkContext jsc, String mongoCollection, Class entity,String pipeline){
        ReadConfig readConfig = getReadConfig(jsc,mongoCollection);
        Dataset mongoData = MongoSpark.load(jsc,readConfig)
                .withPipeline(Collections.singletonList(Document.parse(pipeline)))
                .toDS(entity);
        return mongoData;

    }

    public static ReadConfig getReadConfig(JavaSparkContext jsc, String mongoCollection){
        Map<String,String> readOverrides = new HashMap<>(16);
        readOverrides.put("collection",mongoCollection);
        //从mongo中读数据
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        return readConfig;

    }

    public static Boolean isCollectionEmpty(JavaSparkContext jsc, String mongoCollection){
          ReadConfig readConfig = getReadConfig(jsc,mongoCollection);
          return MongoSpark.load(jsc,readConfig).isEmpty();

    }



}
