package com.hongcha.turtles.common.dto.topic;

import java.util.Set;

public class SubscriptionMessageReq {
    private String groupName;

    private Set<String> topicNames;

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
}
