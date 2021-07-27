package common;


import java.util.Calendar;
import java.util.regex.Pattern;

public class UserDefinedFunction {


    public static String checkContatel(String contatel) {

        String chineseEd = "[\\u4E00-\\u9FFF]+$";
        boolean isReplace = false;
        //排除中文情况
        boolean isValid = Pattern.compile(chineseEd).matcher(contatel).matches();
        if (!isValid) {
            //定义手机正则
            String mobileRegex = "";
            //定义有区号和“-”的固定电话正则
            String telRegex1 = "";
            //定义有区号的固定电话正则
            String telRegex2 = "";
            //定义格式为xx-xxxxx
            String telRegex3 = "";

            //定义纯数字
            String goodNum = "^\\d+$";
            if (contatel.indexOf("-") == -1) {
                if (Pattern.compile(goodNum).matcher(contatel).matches() && Pattern.compile(mobileRegex).matcher(contatel).matches()) {
                    isReplace = true;
                } else if (Pattern.compile(goodNum).matcher(contatel).matches() && Pattern.compile(telRegex2).matcher(contatel).matches()) {
                    isReplace = true;

                } else {
                    String[] contaArray = contatel.split("-");
                    if (contaArray.length > 0) {
                        //中间段
                        String min_code = "";
                        //尾段
                        String end_code = "";

                        for (int i = contaArray.length; i > 0; i--) {
                            end_code = contaArray[contaArray.length - 1];

                            if (contaArray.length >= 2) {
                                end_code = contaArray[contaArray.length - 2];
                                if (Pattern.compile(mobileRegex).matcher(min_code+end_code).matches()) {

                                } else if (Pattern.compile(telRegex1).matcher(min_code+end_code).matches()) {

                                } else if (Pattern.compile(mobileRegex).matcher(min_code+end_code).matches()) {

                                } else if (Pattern.compile(mobileRegex).matcher(min_code+end_code).matches()) {

                                    break;
                                }
                            }
                        }
                    }

                }
                if (!isReplace) {
                    contatel = null;
                }
            }
        }
        return contatel;
    }

    public static void main(String[] args) {
        Calendar cal = Calendar.getInstance();
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH);
        System.out.println(yearNow);
        System.out.println(monthNow);
    }
    public static int toAge(String birthDay) {

        String birthdayArrary[]  = birthDay.split("-");
        int selectYear  = Integer.parseInt(birthdayArrary[0]);
        int selectMonth = Integer.parseInt(birthdayArrary[1]);
        int selectDay   = Integer.parseInt(birthdayArrary[2]);
        //当前时间
        Calendar cal = Calendar.getInstance();
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH)+1;

        int dayNow = cal.get(Calendar.DATE);

        //计算
        int yearSub = yearNow - selectYear;
        int monthSub = monthNow - selectMonth;
        int daySub = dayNow - selectDay;

        int age = yearSub;//大致赋值
        if (yearSub <= 0) {
            age = 0;
        }else if(yearSub>0){
            if (monthSub < 0) {
                age = age -1;
            }else if (monthSub ==0 && daySub<0){
                age = age -1;
            }
        }
        return age;
    }
}
