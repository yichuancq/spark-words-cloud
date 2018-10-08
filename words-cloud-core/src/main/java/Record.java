import java.io.Serializable;

/**
 * 数据对象
 */
public class Record implements Serializable {

    /**
     * 站点id
     */
    private String stationId;
    /**
     * 权重
     */
    private Integer weight;
    /**
     * 消息体
     */
    private String msg;

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }


    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Record{" +
                "stationId='" + stationId + '\'' +
                ", weight=" + weight +
                ", msg='" + msg + '\'' +
                '}';
    }
}
