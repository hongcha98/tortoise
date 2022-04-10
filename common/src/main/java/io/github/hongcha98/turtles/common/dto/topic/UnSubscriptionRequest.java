package io.github.hongcha98.turtles.common.dto.topic;

import java.util.Set;

public class UnSubscriptionRequest {
    private String group;

    private Set<String> topics;

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
}
