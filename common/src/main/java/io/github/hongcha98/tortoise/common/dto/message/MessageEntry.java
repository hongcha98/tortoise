package io.github.hongcha98.tortoise.common.dto.message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息条目
 */
public class MessageEntry {
    // 消息id,由broker生成
    private String id;
    // 创建时间
    private long createTime;
    // 消费次数
    private int consumptionTimes;
    // 消息头
    private Map<String, String> header = new HashMap<>();
    // 消息体
    private byte[] body;
    // 消息所处的offset
    private int offset;
    //下一个消息的offset
    private int nextOffset;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public int getConsumptionTimes() {
        return consumptionTimes;
    }

    public void setConsumptionTimes(int consumptionTimes) {
        this.consumptionTimes = consumptionTimes;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public byte[] getBody() {
        return body == null ? new byte[0] : body;
    }

    public void setBody(byte[] body) {
        this.body = body;
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

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", createTime=" + createTime +
                ", consumptionTimes=" + consumptionTimes +
                ", header=" + header +
                ", body=" + Arrays.toString(body) +
                ", offset=" + offset +
                ", nextOffset=" + nextOffset +
                '}';
    }
}
