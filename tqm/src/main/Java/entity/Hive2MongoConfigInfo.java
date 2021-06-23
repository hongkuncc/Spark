package entity;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

/**
 * @ClassName: Hive2Mongo
 * @Description: 运行相关参数
 * @Author: hongkuncc
 * @Date 2021/6/17 14:36
 * @Version 1.0
 */
public class Hive2MongoConfigInfo {
    private SparkSession sparkSession;
    private JavaSparkContext jsc;
    private Logger log;

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public JavaSparkContext getJsc() {
        return jsc;
    }

    public void setJsc(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    public Logger getLog() {
        return log;
    }

    public void setLog(Logger log) {
        this.log = log;
    }
}
