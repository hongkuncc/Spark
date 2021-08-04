package constant;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * @ClassName: SparkJobConfig
 * @Description: 用于反射的sparkConfig参数类
 * @Author: hongkuncc
 * @Date 2021/8/3 23:02
 * @Version 1.0
 */
public class SparkJobConfig {
    SparkSession sparkSession;
    JavaSparkContext jsc;
    Map<String,String> sqlMap;
    String runMode;
    String sartTime;
    String endTime;

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

    public String getSartTime() {
        return sartTime;
    }

    public String getEndTime() {
        return endTime;
    }


    public SparkJobConfig(SparkSession sparkSession, JavaSparkContext jsc, Map<String, String> sqlMap, String runMode, String sartTime, String endTime) {
        this.sparkSession = sparkSession;
        this.jsc = jsc;
        this.sqlMap = sqlMap;
        this.runMode = runMode;
        this.sartTime = sartTime;
        this.endTime = endTime;
    }

    public SparkJobConfig(SparkSession sparkSession, Map<String, String> sqlMap) {
        this.sparkSession = sparkSession;
        this.sqlMap = sqlMap;
    }
}
