package io.github.hongcha98.turtles.broker.context;

import io.netty.channel.Channel;

import java.util.Map;


public interface ChannelContextManage {
    /**
     * 获取 ChannelContext , 如果没有则添加一个空白的ChannelContext
     *
     * @param channel
     * @return
     */
    ChannelContext getChannelContext(Channel channel);

    /**
     * 删除 ChannelContext
     */
    ChannelContext deleteChannelContext(Channel channel);

    /**
     * 获取所有ChannelContext
     */
    Map<Channel, ChannelContext> getAllChannelContext();
}
