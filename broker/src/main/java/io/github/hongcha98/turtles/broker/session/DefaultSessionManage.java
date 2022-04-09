package io.github.hongcha98.turtles.broker.session;

import io.github.hongcha98.turtles.broker.topic.Topic;
import io.github.hongcha98.turtles.broker.topic.TopicManage;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultSessionManage implements SessionManage {
    private static final Logger log = LoggerFactory.getLogger(DefaultSessionManage.class);

    private final Map<Node, Map<Channel, Set<Integer>>> subscriptionNodeMap = new ConcurrentHashMap<>();

    private final TopicManage topicManage;

    public DefaultSessionManage(TopicManage topicManage) {
        this.topicManage = topicManage;
    }


    @Override
    public void subscription(String topicName, String groupName, Channel channel) {
        Node node = new Node(topicName, groupName);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.computeIfAbsent(node, n -> new ConcurrentHashMap<>());
        if (!channelQueuesMap.containsKey(channel)) {
            channelQueuesMap.computeIfAbsent(channel, c -> new HashSet<>());
            reallocate(node);
        }
    }

    /**
     * 重新分配消费实例
     *
     * @param
     */
    protected void reallocate(Node node) {
        Map<Channel, Set<Integer>> channelQueueIdMap = subscriptionNodeMap.get(node);
        Topic topic = topicManage.getTopic(node.getTopic());
        Set<Integer> queuesId = topic.getQueuesId();
        // 消费者实例个数
        int size = channelQueueIdMap.size();
        if (size == 0) return;
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
        log.info("node :{} , reallocate : {}", node, channelQueueIdMap);
    }

    @Override
    public void unSubscription(String topicName, String groupName, Channel channel) {
        Node node = new Node(topicName, groupName);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.get(node);
        if (channelQueuesMap != null) {
            Set<Integer> queueSet = channelQueuesMap.remove(channel);
            if (queueSet != null && !queueSet.isEmpty()) {
                reallocate(node);
            }
        }

    }

    @Override
    public Set<Integer> getAllocate(String topicName, String groupName, Channel channel) {
        Node node = new Node(topicName, groupName);
        Map<Channel, Set<Integer>> channelQueuesMap = subscriptionNodeMap.get(node);
        if (channelQueuesMap != null) {
            return channelQueuesMap.getOrDefault(channel, Collections.EMPTY_SET);
        }
        return Collections.EMPTY_SET;
    }

    static class Node {

        private String topic;
        private String group;

        public Node(String topic, String group) {
            this.topic = topic;
            this.group = group;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(topic, node.topic) && Objects.equals(group, node.group);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, group);
        }

        @Override
        public String toString() {
            return "Node{" +
                    "topic='" + topic + '\'' +
                    ", group='" + group + '\'' +
                    '}';
        }

    }

}

