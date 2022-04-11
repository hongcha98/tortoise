package io.github.hongcha98.turtles.client.consumer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.AbstractClientApi;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.message.Message;
import io.github.hongcha98.turtles.common.dto.message.request.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.response.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import io.github.hongcha98.turtles.common.dto.offset.request.OffsetCommitRequest;
import io.github.hongcha98.turtles.common.dto.session.request.SubscriptionRequest;
import io.github.hongcha98.turtles.common.error.TurtlesException;

import java.util.HashSet;
import java.util.List;
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
                messageGetRequest.setTopic(topic);
                messageGetRequest.setNumber(getTurtlesConfig().getPullMessageNumber());
                MessageGetResponse messageGetResponse = getCore().pullMessage(messageGetRequest);
                Map<Integer, List<MessageInfo>> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
                if (queueIdMessageMap.isEmpty()) {
                    LOG.debug("topic : {} , group :{} ,no news has been pulled waiting for the next pull", topic, getTurtlesConfig().getGroup());
                    break;
                }
                queueIdMessageMap.forEach((queueId, messageInfos) -> {
                    int offset = -1;
                    try {
                        for (MessageInfo messageInfo : messageInfos) {
                            Message message = messageInfo.getMessage();
                            try {
                                if (messageListener.listener(message)) {
                                    offset = messageInfo.getNextOffset();
                                } else {
                                    offset = messageInfo.getOffset();
                                    break;
                                }
                            } catch (Exception e) {
                                LOG.error("", e);
                                LOG.error("topic : {} , group :{} ,msg id : {} consumer error", topic, getTurtlesConfig().getGroup(), message.getId());
                                offset = messageInfo.getOffset();
                                break;
                            }
                        }
                    } finally {
                        if (offset != -1) {
                            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest();
                            offsetCommitRequest.setTopic(topic);
                            offsetCommitRequest.setQueueId(queueId);
                            offsetCommitRequest.setOffset(offset);
                            getCore().commitOffset(offsetCommitRequest);
                        }
                    }

                });
            }
        } catch (Exception e) {
            LOG.error("", e);
            LOG.debug("topic : {} , group :{} ,pull message error", topic, getTurtlesConfig().getGroup());
        }
    }


    @Override
    public void subscription(String topic, MessageListener messageListener) {
        if (getStart().get() == false) {
            messageListenerMap.put(topic, messageListener);
        } else {
            throw new IllegalStateException("consumer already start");
        }
    }

    @Override
    public void subscription(Set<String> topics, MessageListener messageListener) {
        for (String topic : topics) {
            subscription(topic, messageListener);
        }
    }

    protected void doSubscription() {
        SubscriptionRequest subscriptionRequest = new SubscriptionRequest();
        subscriptionRequest.setGroup(getTurtlesConfig().getGroup());
        subscriptionRequest.setTopics(new HashSet<>(messageListenerMap.keySet()));
        if (!getCore().subscription(subscriptionRequest)) {
            throw new TurtlesException("subscription error");
        }
    }

}
