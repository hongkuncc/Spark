package util;

import entity.ClassConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * @ClassName: ClassConfigMethodUtils
 * @Description: 用于配置类的公共方法
 * @Author: hongkuncc
 * @Date 2021/8/2 22:39
 * @Version 1.0
 */
public class ClassConfigMethodUtils {
    public static void executeSqlCreateOrReplaceTempView(ClassConfig classConfig, String sqlTag) {
        classConfig.getSparkSession().sql(classConfig.getSqlMap().get(sqlTag)).createOrReplaceTempView(sqlTag);

    }

    public static Dataset<Row> excuteSql(ClassConfig classConfig, String sqlTag, int repartitions){
        return classConfig.getSparkSession().sql(classConfig.getSqlMap().get(sqlTag)).repartition(repartitions);
    }
}
