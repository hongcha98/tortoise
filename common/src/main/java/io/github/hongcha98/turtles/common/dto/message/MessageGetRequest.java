package io.github.hongcha98.turtles.common.dto.message;

public class MessageGetRequest {
    /**
     * 主题
     */
    private String topic;

    /**
     * 单个queue拉取的数量
     */
    private int number;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }
}
