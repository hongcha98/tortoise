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
    private String groupName;
    /**
     * 订阅topic列表
     */
    private Set<String> topicNames = new HashSet<>();

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

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(Set<String> topicNames) {
        this.topicNames = topicNames;
    }

    public Channel getChannel() {
        return channel;
    }
}
