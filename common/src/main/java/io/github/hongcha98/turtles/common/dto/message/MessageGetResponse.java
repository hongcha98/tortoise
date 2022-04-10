package io.github.hongcha98.turtles.common.dto.message;

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
