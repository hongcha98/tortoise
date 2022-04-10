package io.github.hongcha98.turtles.broker.topic;

import io.github.hongcha98.turtles.broker.LifeCycle;

import java.util.Map;

/**
 * topic管理
 */
public interface TopicManage extends LifeCycle {
    /**
     * 是否含有topic
     *
     * @param topic
     * @return
     */
    boolean exists(String topic);

    /**
     * getTopic
     */
    Topic getTopic(String topic);

    /**
     * 添加topic
     */
    void addTopic(String topic, int queueNumber);

    /**
     * 删除topic
     *
     * @param topic
     */
    void deleteTopic(String topic);

    /**
     * 获取所有topic
     *
     * @return
     */
    Map<String, Topic> getAllTopic();
}
