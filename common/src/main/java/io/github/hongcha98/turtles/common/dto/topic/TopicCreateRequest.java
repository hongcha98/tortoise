package io.github.hongcha98.turtles.common.dto.topic;

public class TopicCreateRequest {
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
