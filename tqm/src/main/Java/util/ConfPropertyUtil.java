package util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: PropertyUtil
 * @Description: 属性配置文件
 * @Author: hongkuncc
 * @Date 2021/6/21 14:54
 * @Version 1.0
 */
public class ConfPropertyUtil implements Serializable {
    private final static Logger log = LoggerFactory.getLogger(ConfPropertyUtil.class);

    public Map<String, String> loadJobProperties(String property) {
        Map<String, String> jobPropertyConfigMap = new HashMap<>();
        InputStream resourceAsStream = null;
        try {
            Properties env = new Properties();
            env.load(java.nio.file.Files.newInputStream(Paths.get(property)));;
            for (Object keyObj:env.keySet()
                 ) {
                String key = (String) keyObj;
                String value = env.getProperty(key);
                jobPropertyConfigMap.put(key,value);
            }

            } catch (IOException e) {
            log.error("load properties error:" + e);
            } finally{
//            if (resourceAsStream != null) {
                safeClose(resourceAsStream);
//            }

            }
        return jobPropertyConfigMap;

    }

    public static void safeClose(InputStream fis) {
        if (fis != null) {
            try{
                fis.close();
            }catch (IOException e){
                log.error(e.toString());
            }
        }
    }
}