package io.github.hongcha98.turtles.common.dto.message;

public class MessageGetRequest {
    /**
     * 主题名称
     */
    private String topicName;

    /**
     * 单个queue拉取的数量
     */
    private int number;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
