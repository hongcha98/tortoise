package io.github.hongcha98.turtles.client.consumer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.AbstractClientApi;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.message.Message;
import io.github.hongcha98.turtles.common.dto.message.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import io.github.hongcha98.turtles.common.dto.offset.OffsetCommitRequest;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionRequest;
import io.github.hongcha98.turtles.common.error.TurtlesException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PullDefaultConsumer extends AbstractClientApi implements Consumer {
    private Map<String, MessageListener> messageListenerMap = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public PullDefaultConsumer(TurtlesConfig turtlesConfig) {
        super(turtlesConfig);
    }


    public PullDefaultConsumer(TurtlesConfig turtlesConfig, Protocol protocol) {
        super(turtlesConfig, protocol);

    }

    public Map<String, MessageListener> getMessageListenerMap() {
        return Collections.unmodifiableMap(messageListenerMap);
    }

    @Override
    protected void doStart() {
        getCore().setRunnable(() -> doSubscription());
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(messageListenerMap.size());
        messageListenerMap.forEach((topic, messageListener) -> {
            scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> topicPullMessage(topic, messageListener), 0, getTurtlesConfig().getPullMessageInterval(), TimeUnit.MILLISECONDS);
        });
    }

    private void topicPullMessage(String topic, MessageListener messageListener) {
        try {
            for (; ; ) {
                MessageGetRequest messageGetRequest = new MessageGetRequest();
                messageGetRequest.setTopicName(topic);
                MessageGetResponse messageGetResponse = getCore().pullMessage(messageGetRequest);
                Map<Integer, MessageInfo> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
                if (queueIdMessageMap.isEmpty()) {
                    log.debug("topic : {} , group :{} ,no news has been pulled waiting for the next pull", topic, getTurtlesConfig().getGroupName());
                    break;
                }
                queueIdMessageMap.forEach((queueId, messageInfo) -> {
                    Message message = messageInfo.getMessage();
                    if (message != null) {
                        try {
                            if (messageListener.listener(message)) {
                                OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest();
                                offsetCommitRequest.setTopicName(topic);
                                offsetCommitRequest.setQueueId(queueId);
                                offsetCommitRequest.setOffset(messageInfo.getNextOffset());
                                if (!getCore().commitOffset(offsetCommitRequest)) {
                                    log.error("topic : {} , group :{} ,msg id : {} commit error", topic, getTurtlesConfig().getGroupName(), message.getId());
                                }
                            }
                        } catch (Exception e) {
                            log.error("", e);
                            log.error("topic : {} , group :{} ,msg id : {} consumer error", topic, getTurtlesConfig().getGroupName(), message.getId());

                        }
                    }
                });
            }
        } catch (Exception e) {
            log.error("", e);
            log.error("topic : {} , group :{} ,pull message error", topic, getTurtlesConfig().getGroupName());
        }
    }


    @Override
    public void subscription(String topic, MessageListener messageListener) {
        messageListenerMap.put(topic, messageListener);
    }

    @Override
    public void subscription(Set<String> topics, MessageListener messageListener) {
        for (String topic : topics) {
            subscription(topic, messageListener);
        }
    }

    protected void doSubscription() {
        SubscriptionRequest subscriptionRequest = new SubscriptionRequest();
        subscriptionRequest.setGroupName(getTurtlesConfig().getGroupName());
        subscriptionRequest.setTopicNames(new HashSet<>(messageListenerMap.keySet()));
        if (!getCore().subscription(subscriptionRequest)) {
            throw new TurtlesException("subscription error");
        }
    }

}
