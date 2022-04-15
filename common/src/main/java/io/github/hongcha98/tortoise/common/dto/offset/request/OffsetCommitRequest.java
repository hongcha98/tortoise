package io.github.hongcha98.tortoise.common.dto.offset.request;

public class OffsetCommitRequest {
    private String topic;
    private int queueId;
    // 当前偏移量
    private int currentOffset;
    // 要提交的偏移量
    private int commitOffset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public int getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(int commitOffset) {
        this.commitOffset = commitOffset;
    }
}
