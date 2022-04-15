package io.github.hongcha98.tortoise.broker.session;

import io.github.hongcha98.tortoise.broker.topic.Topic;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultSessionManage implements SessionManage {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionManage.class);

    private static final String DELIMITER = "@";

    private final Map<String /* topic@group */, Map<Channel, Set<Integer> /* queue ids*/>> subscriptionNodeMap = new ConcurrentHashMap<>();

    private final TopicManage topicManage;

    public DefaultSessionManage(TopicManage topicManage) {
        this.topicManage = topicManage;
    }


    @Override
    public void subscription(String topic, String group, Channel channel) {
        String key = getKey(topic, group);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.computeIfAbsent(key, n -> new ConcurrentHashMap<>());
        if (!channelQueuesMap.containsKey(channel)) {
            channelQueuesMap.computeIfAbsent(channel, c -> new HashSet<>());
            reallocate(topic, group);
        }
    }

    /**
     * 重新分配消费实例
     *
     * @param
     */
    protected void reallocate(String topic, String group) {
        String key = getKey(topic, group);
        Map<Channel, Set<Integer>> channelQueueIdMap = subscriptionNodeMap.get(key);
        if (!topicManage.exists(topic))
            return;
        Topic tpc = topicManage.getTopic(topic);
        Set<Integer> queuesId = tpc.getQueueIds();
        // 消费者实例个数
        int size = channelQueueIdMap.size();
        if (size == 0) {
            LOG.info("node :{} , no consumers", key);
            return;
        }
        // 平均多少个
        int average = queuesId.size() / size;
        if (average == 0) {
            average = 1;
        }
        int finalAverage = average;
        // 剩余个数
        AtomicInteger surplus = new AtomicInteger(queuesId.size() - average * size);
        // 可分配的queue ids
        Set<Integer> allocatableQueuesId = new HashSet<>(queuesId);
        // 对当前channel的分配id做出调整
        channelQueueIdMap.forEach((channel, assignedQueueIds) -> {
            if (assignedQueueIds.isEmpty()) return;
            int assignedQueueIdsSize = assignedQueueIds.size();
            if (assignedQueueIdsSize > finalAverage) {
                int number = finalAverage;
                if (surplus.getAndDecrement() > 0) {
                    number += 1;
                }
                Iterator<Integer> iterator = assignedQueueIds.iterator();
                while (iterator.hasNext()) {
                    Integer queueId = iterator.next();
                    if (number-- > 0) {
                        allocatableQueuesId.remove(queueId);
                    } else {
                        iterator.remove();
                    }
                }
            } else {
                allocatableQueuesId.removeAll(assignedQueueIds);
            }
        });
        // 对剩余ids 做出分配
        channelQueueIdMap.forEach((channel, assignedQueuesId) -> {
            if (!allocatableQueuesId.isEmpty()) {
                // 需要分配多少个id
                int averageNumber = finalAverage - assignedQueuesId.size();
                if (surplus.getAndDecrement() > 0) {
                    averageNumber += 1;
                }
                for (int i = 0; i < averageNumber; i++) {
                    Iterator<Integer> allocatableQueuesIdIterator = allocatableQueuesId.iterator();
                    if (allocatableQueuesIdIterator.hasNext()) {
                        Integer queueId = allocatableQueuesIdIterator.next();
                        assignedQueuesId.add(queueId);
                        allocatableQueuesIdIterator.remove();
                    }
                }
            }
        });
        LOG.info("node :{} , reallocate : {}", key, channelQueueIdMap);
    }

    @Override
    public void unSubscription(String topic, String group, Channel channel) {
        String key = getKey(topic, group);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.get(key);
        if (channelQueuesMap != null) {
            Set<Integer> queueSet = channelQueuesMap.remove(channel);
            if (queueSet != null && !queueSet.isEmpty()) {
                reallocate(topic, group);
            }
        }

    }

    @Override
    public Set<Integer> getAllocate(String topic, String group, Channel channel) {
        String key = getKey(topic, group);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.get(key);
        if (channelQueuesMap != null) {
            return channelQueuesMap.getOrDefault(channel, Collections.EMPTY_SET);
        }
        return Collections.EMPTY_SET;
    }

    protected String getKey(String topic, String group) {
        return topic + DELIMITER + group;
    }

}

