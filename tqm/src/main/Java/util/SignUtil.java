package util;

import constant.MongoConnectionConstant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * @ClassName: SignUtil
 * @Description: signutil
 * @Author: hongkuncc
 * @Date 2021/6/22 18:14
 * @Version 1.0
 */
public class SignUtil {

    public static String makeSign(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            digest.update(s.getBytes());
            byte[] messageDigest = digest.digest();
            return null;
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    public static String makeSign2(Map<String, Object> data, String key) {
        final StringBuffer sb =new StringBuffer();
        String appCode = (String)data.get(MongoConnectionConstant.APPID);
        sb.append(appCode).append('&').append(key);

        return sb.length()>0?makeSign(sb.toString()):null;
    }
}
