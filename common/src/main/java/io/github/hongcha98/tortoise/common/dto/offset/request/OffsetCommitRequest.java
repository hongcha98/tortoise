package io.github.hongcha98.tortoise.common.dto.offset.request;

public class OffsetCommitRequest {
    private String topic;
    private int queueId;
    private String msgId;
    private int offset;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getOffset() {
        return offset;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
