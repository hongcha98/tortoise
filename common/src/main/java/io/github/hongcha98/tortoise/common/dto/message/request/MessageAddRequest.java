package io.github.hongcha98.tortoise.common.dto.message.request;

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
     * 延时等级  0 <= value <= 16,0为不延时
     */
    private int delayLevel;
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

    public int getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(int delayLevel) {
        if (delayLevel < 0 || delayLevel > 16) {
            throw new IllegalStateException("delay level error : 0 <= level <= 16");
        }
        this.delayLevel = delayLevel;
    }
}
