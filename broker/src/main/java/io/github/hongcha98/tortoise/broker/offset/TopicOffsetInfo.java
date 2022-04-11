package io.github.hongcha98.tortoise.broker.offset;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 偏移信息
 */
public class TopicOffsetInfo {
    /**
     * 消费组
     */
    private String group;
    /**
     * 队列偏移信息
     */
    private Map<Integer/* queue id*/, Integer /* offset*/> queueIdOffsetMap = new ConcurrentHashMap();

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Map<Integer, Integer> getQueueIdOffsetMap() {
        return queueIdOffsetMap;
    }

    public void setQueueIdOffsetMap(Map<Integer, Integer> queueIdOffsetMap) {
        this.queueIdOffsetMap = queueIdOffsetMap;
    }
}
