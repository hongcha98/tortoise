package com.hongcha.turtles.broker.offset;

import com.hongcha.turtles.broker.error.TopicNotExistsException;
import com.hongcha.turtles.broker.topic.Topic;
import com.hongcha.turtles.broker.topic.TopicManage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractOffsetManage implements OffsetManage {
    private Map<String /* topic*/, Map<String /* group */, TopicOffsetInfo>> topicGroupOffsetMap = new ConcurrentHashMap<>();

    private TopicManage topicManage;


    public AbstractOffsetManage(TopicManage topicManage) {
        this.topicManage = topicManage;
    }

    @Override
    public void close() {
        enduranceAll();
    }


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
    public void commitOffset(String topic, String group, int id, int offset) {
        Map<Integer, Integer> offsetMap = getOffset(topic, group);
        offsetMap.put(id, offset);
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
        topicOffsetInfo.setGroupName(group);
        Topic topic1 = topicManage.getTopic(topic);
        topic1.getQueuesId().forEach(id -> {
            topicOffsetInfo.getQueueIdOffsetMap().put(id, topic1.getIdOffset(id));
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
    protected abstract void endurance(String topic, String group, int id, int offset);

    /**
     * 持久化所有
     */
    protected abstract void enduranceAll();

    /**
     * 持久化这个主题的信息
     *
     * @param topic
     */
    protected abstract void enduranceTopic(String topic);

}
