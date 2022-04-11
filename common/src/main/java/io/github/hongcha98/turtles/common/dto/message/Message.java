package io.github.hongcha98.turtles.common.dto.message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息条目
 */
public class Message {
    // 消息id,由broker生成
    private String id;

    // 创建时间
    private long createTime;

    //消息头
    private Map<String, String> header = new HashMap<>();

    // 消息体
    private byte[] body;


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

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", createTime=" + createTime +
                ", header=" + header +
                ", body=" + Arrays.toString(body) +
                '}';
    }


}
