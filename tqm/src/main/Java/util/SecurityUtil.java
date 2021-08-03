package util;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @ClassName: SecurityUtil
 * @Description: 密码模式变更
 * @Author: hongkuncc
 * @Date 2021/8/3 22:\18
 * @Version 1.0
 */
public class SecurityUtil {
    public static String  encrypt(String s,String k) throws NoSuchPaddingException, NoSuchAlgorithmException, BadPaddingException, IllegalBlockSizeException, InvalidKeyException {
        if (s == null|| k == null) {
            throw new IllegalArgumentException();
        }
        SecretKeySpec key = new SecretKeySpec(k.getBytes(),"AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");//创建密码器
        cipher.init(Cipher.ENCRYPT_MODE,key);//初始化
        byte[] result = cipher.doFinal(s.getBytes());//加密
        return HexUtil.byte2hex(result);


    }

    public static String  decrypt(String s,String k) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        if (s == null|| k == null) {
            throw new IllegalArgumentException();
        }
        SecretKeySpec key = new SecretKeySpec(k.getBytes(),"AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");//创建密码器
        cipher.init(Cipher.DECRYPT_MODE,key);//初始化
        byte[] result = cipher.doFinal(HexUtil.hex2byte(s));//加密
        return new String(result);


    }
}
