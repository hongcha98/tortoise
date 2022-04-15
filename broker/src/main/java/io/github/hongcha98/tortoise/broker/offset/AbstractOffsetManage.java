package io.github.hongcha98.tortoise.broker.offset;

import io.github.hongcha98.tortoise.broker.topic.TopicManage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractOffsetManage implements OffsetManage {
    private static final String DELIMITER = "@";

    protected Map<String /* topic@group*/, Map<Integer/* queue id*/, Integer /* offset*/>> topicGroupOffsetMap = new ConcurrentHashMap<>();

    protected TopicManage topicManage;

    protected ScheduledExecutorService scheduledExecutorService;

    public AbstractOffsetManage(TopicManage topicManage) {
        this.topicManage = topicManage;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }

    @Override
    public void start() {
        initAllOffset();
        scheduledExecutorService.scheduleAtFixedRate(() -> enduranceAll(), 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
        doClose();
    }

    protected abstract void doClose();

    @Override
    public Map<Integer, Integer> getMinOffset(String topic) {
        Map<Integer, Integer> offsetMinMap = new HashMap<>();
        topicGroupOffsetMap.forEach((topicGroup, queueIdOffsetMap) -> {
            if (topic.equals(topicGroup.split(DELIMITER)[0])) {
                queueIdOffsetMap.forEach((queueId, offset) -> {
                    Integer min = offsetMinMap.get(queueId);
                    if (min == null || offset < min) {
                        offsetMinMap.put(queueId, offset);
                    }
                });
            }
        });
        return offsetMinMap;
    }

    @Override
    public void deleteTopicOffset(String topic) {
        topicGroupOffsetMap.forEach((topicGroup, queueIdOffsetMap) -> {
            if (topic.equals(topicGroup.split(DELIMITER)[0])) {
                topicGroupOffsetMap.remove(topicGroup);
            }
        });
    }

    protected Map<Integer, Integer> getOffset(String topic, String group) {
        String key = getKey(topic, group);
        return topicGroupOffsetMap.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
    }

    protected String getKey(String topic, String group) {
        return topic + DELIMITER + group;
    }

    @Override
    public int getOffset(String topic, String group, int id) {
        return getOffset(topic, group).computeIfAbsent(id, i -> topicManage.getTopic(topic).getQueueFile(id).getPosition());
    }

    @Override
    public void offsetForward(String topic, int id, int forward) {
        topicGroupOffsetMap.forEach((topicGroup, queueIdOffsetMap) -> {
            if (topic.equals(topicGroup.split(DELIMITER)[0])) {
                Integer offset = queueIdOffsetMap.get(id);
                queueIdOffsetMap.put(id, offset - forward);
            }
        });
        enduranceTopic(topic);
    }


    @Override
    public boolean casCommitOffset(String topic, String group, int id, int oldOffset, int newOffset) {
        Map<Integer, Integer> offsetMap = getOffset(topic, group);
        if (offsetMap.get(id) == oldOffset) {
            offsetMap.put(id, newOffset);
            endurance(topic, group, id, newOffset);
            return true;
        }
        return false;
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

    /**
     * 初始化所有的offset信息
     */
    protected abstract void initAllOffset();

}
