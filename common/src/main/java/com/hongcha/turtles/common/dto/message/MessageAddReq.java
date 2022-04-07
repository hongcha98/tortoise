package com.hongcha.turtles.common.dto.message;

import java.util.HashMap;
import java.util.Map;

public class MessageAddReq {
    private String topicName;
    private Map<String, String> header = new HashMap<>();
    /**
     * 消息数据
     */
    private byte[] body;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
