package com.hongcha.turtles.broker.offset;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 偏移信息
 */
public class TopicOffsetInfo {
    /**
     * 消费组
     */
    private String groupName;
    /**
     * 队列偏移信息
     */
    private Map<Integer/* queue id*/, Integer /* offset*/> queueIdOffsetMap = new ConcurrentHashMap();

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Map<Integer, Integer> getQueueIdOffsetMap() {
        return queueIdOffsetMap;
    }

    public void setQueueIdOffsetMap(Map<Integer, Integer> queueIdOffsetMap) {
        this.queueIdOffsetMap = queueIdOffsetMap;
    }
}
