package entity;

/**
 * @ClassName: ActionLog
 * @Description: visit_action_log实体类
 * @Author: hongkuncc
 * @Date 2021/8/2 23:29
 * @Version 1.0
 */
public class ActionLog {
    private Integer source;
    private Integer agentNo;
    private Integer clientId;

    public Integer getSource() {
        return source;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public Integer getAgentNo() {
        return agentNo;
    }

    public void setAgentNo(Integer agentNo) {
        this.agentNo = agentNo;
    }

    public Integer getClientId() {
        return clientId;
    }

    public void setClientId(Integer clientId) {
        this.clientId = clientId;
    }
}
