package com.hongcha.turtles.broker.context;

import io.netty.channel.Channel;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultChannelContextManage implements ChannelContextManage {
    private static Map<Channel, ChannelContext> channelChannelContextMap = new ConcurrentHashMap<>();


    @Override
    public ChannelContext getChannelContext(Channel channel) {
        return channelChannelContextMap.computeIfAbsent(channel, c -> new ChannelContext(c));
    }

    @Override
    public ChannelContext deleteChannelContext(Channel channel) {
        return channelChannelContextMap.remove(channel);
    }

    @Override
    public Map<Channel, ChannelContext> getAllChannelContext() {
        return Collections.unmodifiableMap(channelChannelContextMap);
    }

}
