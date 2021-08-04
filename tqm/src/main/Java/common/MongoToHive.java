package common;

import com.mongodb.spark.config.ReadConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.JavaSparkContext;

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
     * @Param
     * @Param
     * @Return
     * @MethodName
     */
    public static DataSet getMongoDataset(JavaSparkContext jsc, String mongoCollection, Class entity){
        ReadConfig readConfig = getReadConfig(jsc,mongoCollection);


    }

}
