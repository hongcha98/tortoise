package io.github.hongcha98.turtles.broker.context;

import io.netty.channel.Channel;

import java.util.HashSet;
import java.util.Set;

/**
 * Channel上下文，保存Channel的一些消息
 */
public class ChannelContext {
    /**
     * 是否登录成功
     */
    private boolean loginFlag;
    /**
     * group
     */
    private String group;
    /**
     * 订阅topic列表
     */
    private Set<String> topics = new HashSet<>();

    /**
     * channel
     *
     * @return
     */
    private Channel channel;

    public ChannelContext(Channel channel) {
        this.channel = channel;
    }

    public boolean isLoginFlag() {
        return loginFlag;
    }

    public void setLoginFlag(boolean loginFlag) {
        this.loginFlag = loginFlag;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
