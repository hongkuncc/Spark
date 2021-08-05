package job;

import common.InitSparkSession;
import common.MongoToHive;
import entity.ActionLog;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import util.XMLUtil;

import java.util.Map;


public class Mongodb2HiveForTest extends InitSparkSession{
    public static void main(String[] args) {

        SparkSession sparkSession = new InitSparkSession().getSparkSession("Mongodb2HiveForTest",Mongodb2HiveForTest.class,Boolean.TRUE);
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        //设置对应的mongo集合配置
        String MongoDBCollection = "action_log";
        Dataset dataSet = MongoToHive.getMongoDataset(jsc,MongoDBCollection,ActionLog.class);

        //对临时表做处理或落地

        String TempTable = "MongoDBCollectionTmp";
        dataSet.registerTempTable(TempTable);
        Map<String,String> xmlMap = new XMLUtil().loadJobXml("/xml/SQL.xml");

        sparkSession.sql(xmlMap.get("setDataBase"));
        sparkSession.sql(xmlMap.get("insertActionLog").replace("resultTable",TempTable));


        jsc.close();

    }
}
