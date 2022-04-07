package com.hongcha.hcmq.common.dto.topic;

public class TopicCreateMessageReq {
    private String topicName;
    private int queueNumber;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }
}
