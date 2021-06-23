package util;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @ClassName: XMLUtil
 * @Description: 解析XML工具
 * @Author: hongkuncc
 * @Date 2021/6/21 16:16
 * @Version 1.0
 */
public class XMLUtil implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(ConfPropertyUtil.class);

    /*
     * @Author hongkuncc
     * @Description
     * @Date  2021/6/21 16:18
     * @Param
     * @Param
     * @Return
     * @MethodName
     */

    public Map<String,String> loadJobXml(String xmlPath) {
        SAXReader reader = new SAXReader();
        Document document = null;
        InputStream xml = null;
        try {
            reader.setFeature("http://apche", true);
            reader.setFeature("http:", false);
            reader.setFeature("http://", false);
            xml = this.getClass().getResourceAsStream(xmlPath);
            document = reader.read(xml);

        } catch (DocumentException e) {
            log.error("DocumentException" + e);
        } catch (SAXException e) {
            log.error("sax" + e);
        } finally {
            if (xml != null) {
                safeClose(xml);
            }

        }
        
        Map<String,String> jobXMLConfMap = new HashMap<>();
        Element root = document.getRootElement();
        //遍历子节点，找到对应的jobConf
        findElement(root,jobXMLConfMap);
        return  jobXMLConfMap;
    }

    /*
     * @Author hongkuncc
     * @Description :递归查找element值
    * @Date  2021/6/21 17:07
     * @Param 
     * @Param 
     * @Return 
     * @MethodName 
     */
    public  void findElement(Element element,Map<String,String> confMap) {
        for (Iterator iterInner = element.elementIterator(); iterInner.hasNext(); ) {
            StringBuffer stringBuffer = new StringBuffer();
            Element elementInner =(Element) iterInner.next();
            stringBuffer.append(elementInner.getName());
            if (elementInner.elementIterator().hasNext()) {
                findElement(elementInner,confMap);
            }

            String value = elementInner.getText();
            if (!value.trim().equals("") ) {
                confMap.put(stringBuffer.toString(),value.trim());
            }

        }
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