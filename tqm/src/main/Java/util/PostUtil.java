package util;

import com.alibaba.fastjson.JSONObject;
import constant.MongoConnectionConstant;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName: PostUtil
 * @Description: post请求方法
 * @Author: hongkuncc
 * @Date 2021/6/21 15:55
 * @Version 1.0
 */
public class PostUtil {
    //post请求
    public static String getPasswordFromRemote(String url, String safe,String appId, String folder, String object, String reason, String appkey) {
        try {
            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put(MongoConnectionConstant.APPID,appId);
            bodyMap.put(MongoConnectionConstant.SAFE,safe);
            bodyMap.put(MongoConnectionConstant.FOLDER,folder);
            bodyMap.put(MongoConnectionConstant.OBJECT,object);
            bodyMap.put(MongoConnectionConstant.REASON,reason);
            bodyMap.put(MongoConnectionConstant.SIGN,SignUtil.makeSign2(bodyMap,appkey));


            Map<String, Object> result = (Map<String, Object>) JSONObject.parse(HttpUtil.doPost(url,bodyMap).toString());

            if (result != null&&"200".equals(result.get("code"))) {
                return null;
            }else{
                throw new Exception((String) result.get("msg"));
            }

        } catch (Exception e) {
            throw new RuntimeException("gerpassword", e);
        }
    }
}