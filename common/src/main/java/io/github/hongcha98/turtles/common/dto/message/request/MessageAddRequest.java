package io.github.hongcha98.turtles.common.dto.message.request;

import java.util.HashMap;
import java.util.Map;

public class MessageAddRequest {
    /**
     * topic名称
     */
    private String topic;
    /**
     * header
     */
    private Map<String, String> header = new HashMap<>();
    /**
     * 消息数据
     */
    private byte[] body;
    /**
     * 是否刷盘
     */
    private boolean brush;

    public boolean isBrush() {
        return brush;
    }

    public void setBrush(boolean brush) {
        this.brush = brush;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
