package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName: PropertyUtil
 * @Description: 加载properties进map的方法
 * @Author: hongkuncc
 * @Date 2021/8/3 21:52
 * @Version 1.0
 */
public class PropertyUtil  implements Serializable {
    private final static Logger log = LoggerFactory.getLogger(PropertyUtil.class);

    public Map<String,String> loadJobProperties(String property)  {
        Map<String,String> jobPropertiyConfMap = new HashMap<String,String >();
        InputStream resourceAsStream  = null;

        try {
            Properties env = new Properties();
            env.load(java.nio.file.Files.newInputStream(Paths.get(property)));
            for (Object keyObj:env.keySet()
            ) {
                String key = (String) keyObj;
                String value = env.getProperty(key);
                jobPropertiyConfMap.put(key,value);

            }
        } catch (IOException e) {
            log.error("load prperties error : " + e);
        }finally {
            if (null != resourceAsStream ) {
                safeClose(resourceAsStream);
            }
        }
        return jobPropertiyConfMap;

    }

    public static void safeClose(InputStream fis) {
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
