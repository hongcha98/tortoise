package io.github.hongcha98.turtles.common.dto.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageGetResponse {
    private Map<Integer, MessageInfo> queueIdMessageMap = new ConcurrentHashMap<>();

    public Map<Integer, MessageInfo> getQueueIdMessageMap() {
        return queueIdMessageMap;
    }

    public void setQueueIdMessageMap(Map<Integer, MessageInfo> queueIdMessageMap) {
        this.queueIdMessageMap = queueIdMessageMap;
    }
}
