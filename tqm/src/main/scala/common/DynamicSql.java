package common;

import java.util.List;
import java.util.Map;


import entity.DynamicSqlCondition;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @ClassName: DynamicSql
 * @Description: dynamicsql
 * @Author: hongkuncc
 * @Date 2021/6/16
 * @Version 1.0
 */
public class DynamicSql {
    public static String getExcuteSql(String xmlTagSql, String hiveSourcetTable, List<DynamicSqlCondition> dynamicSqlConditionList) {
        StringBuilder xmlTagSqlStringBuffer = new StringBuilder(xmlTagSql);
        for (DynamicSqlCondition dynamicSqlCondition:dynamicSqlConditionList
             ) {
            if (!StringUtils.isEmpty(xmlTagSql) && !StringUtils.isEmpty(dynamicSqlCondition.getCondition())) {
                String addCondition = dynamicSqlCondition.getCondition();

                if (!StringUtils.isEmpty(dynamicSqlCondition.getReplaceTag())) {
                    addCondition = addCondition.replace(dynamicSqlCondition.getReplaceTag(), dynamicSqlCondition.getReplaceValue());
                }
                xmlTagSqlStringBuffer.append("  ").append(addCondition);
            }

        }
        return xmlTagSqlStringBuffer.toString().replace("paraTable",hiveSourcetTable);
    }

/*
 * @Author hongkuncc
 * @Description :将字符串转为查询条件,如P343，P344，P345转为('P343','P344','P345')
 * @Date
 * @Param
 * @Return java.lang.String
 * @MethodName
 */
    public static String getSiftCondition(String refreshParameters){
        String refreshParmeterSql = null;
        String[] refreshParmeterArray = refreshParameters.split(",");
        StringBuilder shiftConditionStr = new StringBuilder("(");
        for (String refreshParmeter:refreshParmeterArray
             ) {
            if (refreshParmeterArray[0] != null) {
                shiftConditionStr.append("'").append(refreshParmeter).append("'");
                refreshParmeterArray[0] = null;
            } else
            {
                shiftConditionStr.append(",").append("'").append(refreshParmeter).append("'");
            }
        }
        shiftConditionStr.append(")");
        refreshParmeterSql = shiftConditionStr.toString();
        return refreshParmeterSql;
    }


    /*
     * @Author hongkuncc
     * @Description
     * @Date
     * @Param
     * @Return
     * @MethodName
     */

    public static String getUnionSourceData(SparkSession sparkSession, Map<String,String> sqlMap,String headSqlTag) {
        //获取默认sql
        StringBuilder resultSqlBuffer = new StringBuilder(sqlMap.get(headSqlTag));
        //读取日志表中记录的更新时间，根据记录更新时间判断是否要将对应数据源加入动态SQL中
        Dataset<Row> ds = sparkSession.sql("select * from bas_lbs_pol_info");
        for (Row row:ds.collectAsList()
             ) {
            String unionAllSql = headSqlTag + row.getAs("branch_tag");
            //动态sql组装
            if (null != sqlMap.get(unionAllSql)) {
                resultSqlBuffer.append(" union all ").append(sqlMap.get(unionAllSql));
            }
        }
        return resultSqlBuffer.toString();
    }


    /*
     * @Author hongkuncc
     * @Description
     * @Date
     * @Param headSqkTag 需要unionall sql 的默认sql,一般为寿险数据源
     * @Param repartition union all 后重分区数量，一般为：shuffle配置的倍数
     * @Param viewNmae 执行union all 后临时视图的名称
     * @Param
     * @Param
     * @Return
     * @MethodName
     */
    public static void getUnionSourceData(SparkSession sparkSession,Map<String,String> sqlMap,String headSqlTag,int repartition,String viewName) {

        //获取默认sql
        Dataset<Row> headDateset = sparkSession.sql(sqlMap.get(headSqlTag));
        //读取日志表中记录的更新时间，根据更新时间判断是否要将对应数据源加入动态sql中
        Dataset<Row> ds = sparkSession.sql("select * from bas_lbs_pol_ben");

        for (Row row:ds.collectAsList()
             ) {
            String unionAllSql = headSqlTag + row.getAs("branch_tag");
            //动态sql组装
            if (null != sqlMap.get(unionAllSql)) {
                Dataset<Row> unionAllDateSet = sparkSession.sql(sqlMap.get(unionAllSql));
                headDateset = headDateset.unionAll(unionAllDateSet).repartition(repartition);
            }
        }
        headDateset.createOrReplaceTempView(viewName);

    }
}
