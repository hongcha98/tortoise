package com.hongcha.turtles.client.consumer;

import com.hongcha.remote.protocol.Protocol;
import com.hongcha.turtles.client.AbstractClientApi;
import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.common.dto.message.Message;
import com.hongcha.turtles.common.dto.message.MessageGetReq;
import com.hongcha.turtles.common.dto.message.MessageGetResp;
import com.hongcha.turtles.common.dto.offset.OffsetCommitReq;
import com.hongcha.turtles.common.dto.topic.SubscriptionMessageReq;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultConsumer extends AbstractClientApi implements Consumer {
    private Map<String, MessageListener> messageListenerMap = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public DefaultConsumer(TurtlesConfig turtlesConfig) {
        super(turtlesConfig);
        getCore().setRunanber(() -> doSubscription());
    }


    public DefaultConsumer(TurtlesConfig turtlesConfig, Protocol protocol) {
        super(turtlesConfig, protocol);
        getCore().setRunanber(() -> doSubscription());

    }

    public Map<String, MessageListener> getMessageListenerMap() {
        return Collections.unmodifiableMap(messageListenerMap);
    }

    @Override
    protected void doStart() throws Exception {
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(messageListenerMap.size());
        messageListenerMap.forEach((topic, messageListener) -> {
            scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> topicPullMessage(topic, messageListener), 100, 100, TimeUnit.MILLISECONDS);
        });
    }

    private void topicPullMessage(String topic, MessageListener messageListener) {
        MessageGetReq messageGetReq = new MessageGetReq();
        messageGetReq.setTopicName(topic);
        MessageGetResp messageGetResp = getCore().pullMessage(messageGetReq);
        messageGetResp.getQueueIdMessageMap().forEach((queueId, messageInfo) -> {
            Message message = messageInfo.getMessage();
            if (message != null) {
                try {
                    messageListener.listener(message);
                    OffsetCommitReq offsetCommitReq = new OffsetCommitReq();
                    offsetCommitReq.setTopicName(topic);
                    offsetCommitReq.setQueueId(queueId);
                    offsetCommitReq.setOffset(messageInfo.getNextOffset());
                    if (!getCore().commitOffset(offsetCommitReq)) {
                        log.error("topic : {} , group :{} ,msg id : {} commit error", topic, getTurtlesConfig().getGroupName(), message.getId());
                    }
                } catch (Exception e) {
                    log.error("topic : {} , group :{} ,msg id : {} consumer error", topic, getTurtlesConfig().getGroupName(), message.getId());
                }
            }
        });
    }

    @Override
    protected void doClose() {

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
        SubscriptionMessageReq subscriptionMessageReq = new SubscriptionMessageReq();
        subscriptionMessageReq.setGroupName(getTurtlesConfig().getGroupName());
        subscriptionMessageReq.setTopicNames(messageListenerMap.keySet());
        if (!getCore().subscription(subscriptionMessageReq)) {
            log.warn("subscription error");
        }
    }

}
