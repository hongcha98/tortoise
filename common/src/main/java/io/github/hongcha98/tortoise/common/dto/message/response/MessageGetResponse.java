package io.github.hongcha98.tortoise.common.dto.message.response;

import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageGetResponse {
    private Map<Integer, List<MessageEntry>> queueIdMessageMap = new ConcurrentHashMap<>();

    public Map<Integer, List<MessageEntry>> getQueueIdMessageMap() {
        return queueIdMessageMap;
    }

    public void setQueueIdMessageMap(Map<Integer, List<MessageEntry>> queueIdMessageMap) {
        this.queueIdMessageMap = queueIdMessageMap;
    }
}
