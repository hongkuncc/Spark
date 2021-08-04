package common;

import constant.ShellParameterConstant;
import entity.ClassConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * @ClassName: MethodInvokable
 * @Description: 通过反射类方法，让任务可以智能选择执行某个方法，或者全部方法
 * @Author: hongkuncc
 * @Date 2021/8/3 22:53
 * @Version 1.0
 */
public class MethodInvokable {
    public static void runMethod(Class runMethodClass, ClassConfig classConfig, Logger log, String runMode, List<String> runningMethodList) {
        try {
            if (StringUtils.equals(ShellParameterConstant.ALLTABLE, runMode)) {
                Class classType = runMethodClass;
                Object invokertester = null;
                for (String runningMethod : runningMethodList
                ) {
                    log.info("====================开始计算====================");
                    Method method = classType.getMethod(runningMethod, new Class[]{ClassConfig.class});
                    method.invoke(invokertester, new Object[]{classConfig});

                }
            }else{
                int i = Integer.parseInt(runMode.substring(2, 4));
                String runningMethod = runningMethodList.get(1);
                log.info("===============传入参数：{}=======开始计算=============");
                Class classType = runMethodClass;
                Object invokertester = classType.newInstance();
                //反射获取方法名
                Method method = classType.getMethod(runningMethod, new Class[]{ClassConfig.class});
                //传入默认参数，调用内部类中对应方法
                method.invoke(invokertester, new Object[]{classConfig});
            }

        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
