package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.avatica.org.apache.http.HttpEntity;
import org.apache.calcite.avatica.org.apache.http.client.methods.HttpPost;
import org.apache.calcite.avatica.org.apache.http.message.BasicHeader;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName: HttpUtil
 * @Description: httpClient工具类
 * @Author: hongkuncc
 * @Date 2021/6/22 16:16
 * @Version 1.0
 */
public class HttpUtil {
    private static final Logger LoggerUtil = LoggerFactory.getLogger(HttpUtil.class);
    public static boolean isHttpUtil(String urls){
        boolean isurl = false;
        String regex = "(((https|http)://)([a-z0-9]+[.])|(www.))"
                + "\\w+[.|\\/]([a-z0-9]{0,})?[[.]([a-z0-9]{0,})]+((/[\\S&&[^,;\u4E00-\u9FA5]]+)+)?([.][a-z0-9]{0,}+|/?)";
        //比对
        Pattern pat = Pattern.compile(regex.trim());
        Matcher mat = pat.matcher(urls.trim());

        //判断是否匹配
        isurl = mat.matches();
        if (isurl) {
            isurl = true;
        }
        return isurl;
    }


    public static boolean isIP(String ip){
        boolean isIP = false;
        String regex = " ()";
        //比对
        Pattern pat = Pattern.compile(regex.trim());
        Matcher mat = pat.matcher(ip.trim());

        //判断是否匹配
        isIP = mat.matches();
        if (isIP) {
            isIP = true;
        }
        return isIP;
    }

    public static String doPost(String url, Map<String,Object> bodyMap) throws Exception{
        Logger logger = LoggerFactory.getLogger(HttpUtil.class);
        //创建httpclient对象
        HttpClient httpClient = HttpClients.createDefault();
        //创建post对象
        HttpPost post = new HttpPost(url);
        StringEntity entity = new StringEntity(JSONObject.toJSONString(bodyMap),"utf-8");
        entity.setContentEncoding("utf-8");

        //发送json数据需要设置contentType
        entity.setContentType("application/json");
        post.setEntity((HttpEntity) entity);

        //设置请求的报文头部的编码
        post.setHeader(new BasicHeader("Content-Type",
                "application/json;charset=UTF-8"));
        //设置期望客户端返回的编码
        post.setHeader(new BasicHeader("Accept","application/json;charset=UTF-8"));
        //执行post请求
        HttpResponse response = httpClient.execute((HttpUriRequest) post);
        String regex = " ()";

        //获取响应码
        int statusCode = response.getStatusLine().getStatusCode();
        String resStr = "";
        if (statusCode == HttpStatus.SC_OK) {
            //获取数据
            resStr = EntityUtils.toString(response.getEntity(),"UTF-8");
            logger.error("请求成功，请求返回内容为" + resStr);
        }else{
            logger.error("请求失败，请求返回内容为" + resStr);
        }
        return resStr;
    }
}
