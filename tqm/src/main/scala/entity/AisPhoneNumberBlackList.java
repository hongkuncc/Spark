package entity;

/**
 * @ClassName: AisPhoneNumberBlackList
 * @Description: json
 * @Author: hongkuncc
 * @Date 2021/6/17 14:40
 * @Version 1.0
 */
public class AisPhoneNumberBlackList {
/*    {
        "_id" : "",
        "phone_number":"2018-05-29 12:49:51.0",
        "mark":"1",
        "update_time":"2021-06-17 14:44:57",
    }*/
    private String _id;
    private String phone_number;
    private String last_undwrt_date;
    private String mark;
    private String update_time;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(String phone_number) {
        this.phone_number = phone_number;
    }

    public String getLast_undwrt_date() {
        return last_undwrt_date;
    }

    public void setLast_undwrt_date(String last_undwrt_date) {
        this.last_undwrt_date = last_undwrt_date;
    }

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }
}
