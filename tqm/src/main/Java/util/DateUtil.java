package util;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName: Date
 * @Description: 常用date
 * @Author: hongkuncc
 * @Date 2021/6/21 14:50
 * @Version 1.0
 */
public class DateUtil {

/*    public static DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern({"yyyyMMddHHmmss");
    public static DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern({"yyyy-MM-dd");
    public static DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern({"yyyyMMdd hh:mm:ss");*/

    public static void main(String[] args) {
        String str = "20201213";
        System.out.println(str.substring(4,6));
/*
        System.out.println(DateUtil.getTimeStamp("20201201","20201213"));
*/
    }


    public static Long getTimeStamp(String calculateDate,String calculateHour)  {
        String str = calculateDate+ calculateHour.substring(4,6) + "0000";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(str);

        }catch (ParseException e){
            e.printStackTrace();
        }
        long ts = date.getTime();

        return ts;
    }
}
