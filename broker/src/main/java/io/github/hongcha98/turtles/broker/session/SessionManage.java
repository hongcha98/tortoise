package io.github.hongcha98.turtles.broker.session;


import io.netty.channel.Channel;

import java.util.Set;

public interface SessionManage {
    /**
     * 订阅
     *
     * @param topic
     * @param group
     * @param channel
     */
    void subscription(String topic, String group, Channel channel);

    /**
     * 取消订阅
     *
     * @param topic
     * @param group
     * @param channel
     */
    void unSubscription(String topic, String group, Channel channel);


    /**
     * 获取分配信息
     *
     * @param topic
     * @param group
     * @param channel
     * @return
     */
    Set<Integer> getAllocate(String topic, String group, Channel channel);


}
