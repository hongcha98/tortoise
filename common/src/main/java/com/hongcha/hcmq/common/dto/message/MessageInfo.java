package com.hongcha.hcmq.common.dto.message;

public class MessageInfo {
    /**
     * 消息
     */
    private Message message;
    /**
     * 消息所处的offset
     */
    private int offset;
    /**
     * 下一个消息的offset
     */
    private int nextOffset;


    public MessageInfo(Message message, int offset, int nextOffset) {
        this.message = message;
        this.offset = offset;
        this.nextOffset = nextOffset;
    }

    public Message getMessage() {
        return message;
    }

    public int getOffset() {
        return offset;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "message=" + message +
                ", offset=" + offset +
                ", nextOffset=" + nextOffset +
                '}';
    }
}

