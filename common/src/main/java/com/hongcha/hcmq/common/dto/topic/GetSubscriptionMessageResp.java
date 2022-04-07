package com.hongcha.hcmq.common.dto.topic;

import java.util.Map;
import java.util.Set;

public class GetSubscriptionMessageResp {

    Map<String, Set<Integer>> topicQueuesIdMap;

    public Map<String, Set<Integer>> getTopicQueuesIdMap() {
        return topicQueuesIdMap;
    }

    public void setTopicQueuesIdMap(Map<String, Set<Integer>> topicQueuesIdMap) {
        this.topicQueuesIdMap = topicQueuesIdMap;
    }

    @Override
    public String toString() {
        return "GetSubscriptionMessageResp{" +
                "topicQueuesIdMap=" + topicQueuesIdMap +
                '}';
    }
}
