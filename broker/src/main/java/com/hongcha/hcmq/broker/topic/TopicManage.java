package com.hongcha.hcmq.broker.topic;

import com.hongcha.hcmq.broker.LifeCycle;

import java.util.Map;

/**
 * topic管理
 */
public interface TopicManage extends LifeCycle {
    /**
     * 是否含有topic
     *
     * @param topicName
     * @return
     */
    boolean exists(String topicName);

    /**
     * getTopic
     */
    Topic getTopic(String topicName);

    /**
     * 添加topic
     */
    void addTopic(String topicName, int queueNumber);

    /**
     * 删除topic
     *
     * @param topicName
     */
    void deleteTopic(String topicName);

    /**
     * 获取所有topic
     *
     * @return
     */
    Map<String, Topic> getAllTopic();
}
