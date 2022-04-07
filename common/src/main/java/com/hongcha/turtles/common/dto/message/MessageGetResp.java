package com.hongcha.turtles.common.dto.message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageGetResp {
    private Map<Integer, MessageInfo> queueIdMessageMap = new ConcurrentHashMap<>();

    public Map<Integer, MessageInfo> getQueueIdMessageMap() {
        return queueIdMessageMap;
    }

    public void setQueueIdMessageMap(Map<Integer, MessageInfo> queueIdMessageMap) {
        this.queueIdMessageMap = queueIdMessageMap;
    }
}
