package io.github.hongcha98.tortoise.common.dto.topic.request;

public class TopicCreateRequest {
    private String topic;
    private int queueNumber;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }
}
