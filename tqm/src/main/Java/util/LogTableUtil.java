package util;

import org.apache.spark.sql.SparkSession;

import static util.DateUtil.getAddDate;

/**
 * @ClassName: LogTableUtil
 * @Description: 读，写日志表工具类
 * @Author: hongkuncc
 * @Date 2021/6/22 17:21
 * @Version 1.0
 */
public class LogTableUtil {

    /*
     * @Author hongkuncc
     * @Description 写表操作
     * @Date  2021/8/2 23:53
     * @Param
     * @Param
     * @Return
     * @MethodName
     */
    public static void writeTableOperateSql(SparkSession sparkSession,String tableOperate,String tableName) {
        String paramSql = "insert overwrite table paramTable " +
        "select 'paramOperate' as operate " +
        ",'paramUpdateTime' as update_time " +
        "from default.dual";
        String excuteSql =  paramSql.replace("paramTanle",tableName)
                .replace("paramOperate",tableOperate)
                .replace("paramUpdateTime",getAddDate(0));
        sparkSession.sql(excuteSql);

    }
    /*
     * @Author hongkuncc
     * @Description 读b表操作
     * @Date  2021/8/2 23:53
     * @Param
     * @Param
     * @Return
     * @MethodName
     */

    public static void readTableOperateSql(SparkSession sparkSession,String tableOperate,String tableName) {

        String paramSql = "select 1 from paramTable \n"+
                "where ";
        String excuteSql =  paramSql.replace("paramTanle",tableName)
                .replace("paramOperate",tableOperate)
                .replace("paramUpdateTime",getAddDate(0));
        sparkSession.sql(excuteSql);
    }

}
