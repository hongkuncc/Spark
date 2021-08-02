package entity;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @ClassName: ClassConfig
 * @Description: ClassConfig
 * @Author: hongkuncc
 * @Date 2021/8/2 22:45
 * @Version 1.0
 */
public class ClassConfig {
    SparkSession sparkSession;
    JavaSparkContext jsc;
    Map<String,String> sqlMap;
    String runMode;
    String startTime;
    String endTime;

    public ClassConfig(SparkSession sparkSession, Map<String, String> sqlMap) {
        this.sparkSession = sparkSession;
        this.sqlMap = sqlMap;
    }

    public ClassConfig(SparkSession sparkSession, JavaSparkContext jsc, Map<String, String> sqlMap, String runMode, String startTime, String endTime) {
        this.sparkSession = sparkSession;
        this.jsc = jsc;
        this.sqlMap = sqlMap;
        this.runMode = runMode;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public JavaSparkContext getJsc() {
        return jsc;
    }

    public Map<String, String> getSqlMap() {
        return sqlMap;
    }

    public String getRunMode() {
        return runMode;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }
}
