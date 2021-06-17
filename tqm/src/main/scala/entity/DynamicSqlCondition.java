package entity;

/**
 * @ClassName: DynamicSqlCondition
 * @Description: 动态sql需要加入增加条件的实体类
 * @Author: hongkuncc
 * @Date 2021/6/16
 * @Version 1.0
 */
public class DynamicSqlCondition {
    private String condition;
    private String replaceTag;
    private String replaceValue;

    public DynamicSqlCondition(String condition, String replaceTag, String replaceValue) {
        this.condition = condition;
        this.replaceTag = replaceTag;
        this.replaceValue = replaceValue;
    }

    public DynamicSqlCondition(){

    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getReplaceTag() {
        return replaceTag;
    }

    public void setReplaceTag(String replaceTag) {
        this.replaceTag = replaceTag;
    }

    public String getReplaceValue() {
        return replaceValue;
    }

    public void setReplaceValue(String replaceValue) {
        this.replaceValue = replaceValue;
    }
}
