package io.github.hongcha98.tortoise.common.dto.message;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 消息条目
 */
public class Message {
    // 消息id,由broker生成
    private String id;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(id, message.id) && Objects.equals(header, message.header) && Arrays.equals(body, message.body);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, header);
        result = 31 * result + Arrays.hashCode(body);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", header=" + header +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
