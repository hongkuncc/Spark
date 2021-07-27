/*
package job;

import common.DynamicSql;
import common.Hive2Mongo;
import common.InitSparkSession;
import entity.DynamicSqlCondition;
import entity.Hive2MongoConfigInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.XMLUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static constant.ShellParameterConstant.GROUP;

*/
/**
 * @ClassName: Hive2Mongo
 * @Description: 解析xml写入
 * @Author: hongkuncc
 * @Date 2021/6/21 16:06
 * @Version 1.0
 *//*

public class Hive2MongoFromControl extends InitSparkSession {
    private static final Logger log = LoggerFactory.getLogger(Hive2MongoFromControl.class);


    String isGroup = args[0];
    String isPartition = args[1];
    public static void main(String[] args) {
        SparkSession sparkSession = new InitSparkSession().getSparkSession("Hive2Mongo4ChannelControl",Hive2MongoFromControl.class,Boolean.TRUE);
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        Map<String,String> xmlMap = new XMLUtil().loadJobXml("/xml/SQL.xml");

        Hive2MongoConfigInfo hive2MongoConfigInfo = new Hive2MongoConfigInfo();
        hive2MongoConfigInfo.setSparkSession(sparkSession);
        hive2MongoConfigInfo.setJsc(jsc);
        hive2MongoConfigInfo.setLog((java.util.logging.Logger) log);

        sparkSession.sql(xmlMap.get("setDataBase"));

        if (StringUtils.equals(isGroup,GROUP)) {

        }


        jsc.close();
        sparkSession.stop();
    }

    */
/*
     * @Author hongkuncc
     * @Description 根据指定表导入mongo
     * @Date  2021/6/23 17:32
     * @Param
     * @Param
     * @Return
     * @MethodName
     *//*

    public static void hive2Mongo(JavaSparkContext jsc, Map<String,String> xmlMap, Hive2MongoConfigInfo hive2MongoConfigInfo,List<DynamicSqlCondition> dynamicSqlConditionList,String sourceTabel,String runMode,String outputTable,String limitTag) {
        if (!StringUtils.equals(sourceTabel,"frey_pol_ben")) {
            String selectPolben = xmlMap.get("selectPolBne");
            for (int i = 0; i < 10; i++) {
                String executeSql = getExecuteSql(selectPolben,"frey_pol_ben_" + i,dynamicSqlConditionList,"noGroup","pol_info");
                Hive2Mongo.hive2Mongo(hive2MongoConfigInfo,executeSql,outputTable);
                if (StringUtils.equals(limitTag,"2019")) {
                    i = 10;
                }
            }
        }
        
    }

    public static String getExecuteSql(String xmlTagSql, String hiveSourceTable, List<DynamicSqlCondition> dynamicSqlConditionList,String isGroup,String field) {
        StringBuilder xmlTagSqlStringBuffer = new StringBuilder(xmlTagSql);
        for (DynamicSqlCondition dynamicSqlCondition:dynamicSqlConditionList
             ) {
            if (!StringUtils.isEmpty(xmlTagSql) && !StringUtils.isEmpty(dynamicSqlCondition.getCondition())) {
                String addCondition = dynamicSqlCondition.getCondition();
                if (!StringUtils.isEmpty(dynamicSqlCondition.getReplaceTag())) {
                    if (StringUtils.equals(isGroup,GROUP)) {
                        addCondition = addCondition
                                .replace(dynamicSqlCondition.getReplaceTag(),dynamicSqlCondition.getReplaceValue()
                                .replace("field",field));
                    }else{
                        addCondition = addCondition
                                .replace(dynamicSqlCondition.getReplaceTag(),dynamicSqlCondition.getReplaceValue());
                    }
                }
                xmlTagSqlStringBuffer.append(" ").append(addCondition);
            }
        }
        return xmlTagSqlStringBuffer.toString().replace("paramTable",hiveSourceTable);
    }

    public static void configSourceTable(List<String> sourceTableList){
        //定义计算源表
        sourceTableList.add("frey_pol_ben");
        sourceTableList.add("frey_pol_ben");
        sourceTableList.add("frey_pol_ben");
        sourceTableList.add("frey_pol_ben");
    }

    public static Map<String,String> configResultTableMap(){
        Map<String,String> resultTableMap = new HashMap<>(16);
        resultTableMap.put("frey_pol_ben","pol_ben");
        resultTableMap.put("frey_pol_ben","pol_ben");
        resultTableMap.put("frey_pol_ben","pol_ben");
        resultTableMap.put("frey_pol_ben","pol_ben");
        resultTableMap.put("frey_pol_ben","pol_ben");
        resultTableMap.put("frey_pol_ben","pol_ben");
        return resultTableMap;
    }
}
*/
