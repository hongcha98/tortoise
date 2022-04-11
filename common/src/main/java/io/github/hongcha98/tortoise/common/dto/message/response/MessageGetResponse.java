package io.github.hongcha98.tortoise.common.dto.message.response;

import io.github.hongcha98.tortoise.common.dto.message.MessageInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageGetResponse {
    private Map<Integer, List<MessageInfo>> queueIdMessageMap = new ConcurrentHashMap<>();

    public Map<Integer, List<MessageInfo>> getQueueIdMessageMap() {
        return queueIdMessageMap;
    }

    public void setQueueIdMessageMap(Map<Integer, List<MessageInfo>> queueIdMessageMap) {
        this.queueIdMessageMap = queueIdMessageMap;
    }
}
