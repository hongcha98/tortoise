package io.github.hongcha98.tortoise.broker.offset;

import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.github.hongcha98.tortoise.common.error.TopicNotExistsException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractOffsetManage implements OffsetManage {
    private Map<String /* topic*/, Map<String /* group */, TopicOffsetInfo>> topicGroupOffsetMap = new ConcurrentHashMap<>();

    private TopicManage topicManage;

    private ScheduledExecutorService scheduledExecutorService;

    public AbstractOffsetManage(TopicManage topicManage) {
        this.topicManage = topicManage;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        doClose();
    }

    protected abstract void doClose();


    public Map<String, Map<String, TopicOffsetInfo>> getTopicGroupOffsetMap() {
        return topicGroupOffsetMap;
    }

    public synchronized void setTopicGroupOffsetMap(Map<String, Map<String, TopicOffsetInfo>> topicGroupOffsetMap) {
        this.topicGroupOffsetMap = topicGroupOffsetMap;
    }

    public TopicManage getTopicManage() {
        return topicManage;
    }

    @Override
    public Map<Integer, Integer> getMinOffset(String topic) {
        Map<Integer, Integer> offsetMinMap = new HashMap<>();
        Map<String, TopicOffsetInfo> groupOffsetMap = topicGroupOffsetMap.get(topic);
        if (groupOffsetMap != null) {
            for (TopicOffsetInfo topicOffsetInfo : groupOffsetMap.values()) {
                Map<Integer, Integer> queueIdOffsetMap = topicOffsetInfo.getQueueIdOffsetMap();
                queueIdOffsetMap.forEach((queueId, offset) -> {
                    Integer min = offsetMinMap.get(queueId);
                    if (min == null || offset < min) {
                        offsetMinMap.put(queueId, offset);
                    }
                });
            }
        }
        return offsetMinMap;
    }

    @Override
    public void deleteTopicOffset(String topic) {
        Map<String, TopicOffsetInfo> groupOffsetMap = topicGroupOffsetMap.remove(topic);
        if (groupOffsetMap != null) {
            enduranceTopic(topic);
        }
    }

    @Override
    public Map<Integer, Integer> getOffset(String topic, String group) {
        checkTopic(topic);
        TopicOffsetInfo topicOffsetInfo = topicGroupOffsetMap.computeIfAbsent(topic, t -> new ConcurrentHashMap<>()).get(group);
        if (topicOffsetInfo == null) {
            topicOffsetInfo = initTopicGroupOffset(topic, group);
        }
        return topicOffsetInfo.getQueueIdOffsetMap();
    }

    @Override
    public int getOffset(String topic, String group, int id) {
        return getOffset(topic, group).get(id);
    }

    @Override
    public void offsetForward(String topic, int id, int forward) {
        Map<String, TopicOffsetInfo> groupOffsetMap = topicGroupOffsetMap.get(topic);
        if (groupOffsetMap != null) {
            for (TopicOffsetInfo topicOffsetInfo : groupOffsetMap.values()) {
                Map<Integer, Integer> queueIdOffsetMap = topicOffsetInfo.getQueueIdOffsetMap();
                Integer oldOffset = queueIdOffsetMap.get(id);
                if (oldOffset != null) {
                    queueIdOffsetMap.put(id, oldOffset - forward);
                }
            }
        }
    }

    @Override
    public void commitOffset(String topic, String group, int id, int offset) {
        Map<Integer, Integer> offsetMap = getOffset(topic, group);
        offsetMap.put(id, offset);
        endurance(topic, group, id, offset);
    }

    public void checkTopic(String topic) {
        if (!topicManage.exists(topic)) {
            Map<String, TopicOffsetInfo> groupOffsetMap = topicGroupOffsetMap.remove(topic);
            if (groupOffsetMap != null) {
                enduranceTopic(topic);
            }
            throw new TopicNotExistsException(topic);
        }
    }

    protected TopicOffsetInfo initTopicGroupOffset(String topic, String group) {
        TopicOffsetInfo topicOffsetInfo = new TopicOffsetInfo();
        topicOffsetInfo.setGroup(group);
        Topic tpc = topicManage.getTopic(topic);
        tpc.getQueuesId().forEach(id -> {
            topicOffsetInfo.getQueueIdOffsetMap().put(id, tpc.getIdOffset(id));
        });
        topicGroupOffsetMap.get(topic).put(group, topicOffsetInfo);
        enduranceTopic(topic);
        return topicOffsetInfo;
    }

    /**
     * 持久化这个offset
     *
     * @param topic
     * @param group
     * @param id
     * @param offset
     */
    protected void endurance(String topic, String group, int id, int offset) {
    }

    /**
     * 持久化所有
     */
    protected void enduranceAll() {
    }


    /**
     * 持久化这个主题的信息
     *
     * @param topic
     */
    protected void enduranceTopic(String topic) {
    }

    @Override
    public void start() {
        initAllOffset();
        scheduledExecutorService.scheduleAtFixedRate(() -> enduranceAll(), 1, 1, TimeUnit.MILLISECONDS);
    }

    protected abstract void initAllOffset();

}
