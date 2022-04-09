package io.github.hongcha98.turtles.broker.session;


import io.netty.channel.Channel;

import java.util.Set;

public interface SessionManage {
    /**
     * 订阅
     *
     * @param topicName
     * @param groupName
     * @param channel
     */
    void subscription(String topicName, String groupName, Channel channel);

    /**
     * 取消订阅
     *
     * @param topicName
     * @param groupName
     * @param channel
     */
    void unSubscription(String topicName, String groupName, Channel channel);


    /**
     * 获取分配信息
     *
     * @param topicName
     * @param groupName
     * @param channel
     * @return
     */
    Set<Integer> getAllocate(String topicName, String groupName, Channel channel);


}
