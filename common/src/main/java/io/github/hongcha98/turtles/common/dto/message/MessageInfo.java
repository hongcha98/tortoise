package io.github.hongcha98.turtles.common.dto.message;

public class MessageInfo {
    // 消息
    private Message message;

    // 消费次数
    private int consumptionTimes;

    // 消息所处的offset
    private int offset;

    //下一个消息的offset
    private int nextOffset;


    public MessageInfo() {

    }

    public MessageInfo(Message message, int consumptionTimes, int offset, int nextOffset) {
        this.message = message;
        this.consumptionTimes = consumptionTimes;
        this.offset = offset;
        this.nextOffset = nextOffset;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public int getConsumptionTimes() {
        return consumptionTimes;
    }

    public void setConsumptionTimes(int consumptionTimes) {
        this.consumptionTimes = consumptionTimes;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(int nextOffset) {
        this.nextOffset = nextOffset;
    }
}

