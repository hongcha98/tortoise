package io.github.hongcha98.tortoise.client.consumer;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.client.AbstractClientApi;
import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageGetRequest;
import io.github.hongcha98.tortoise.common.dto.message.response.MessageGetResponse;
import io.github.hongcha98.tortoise.common.dto.offset.request.OffsetCommitRequest;
import io.github.hongcha98.tortoise.common.dto.session.request.SubscriptionRequest;
import io.github.hongcha98.tortoise.common.error.TortoiseException;

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

    public PullDefaultConsumer(TortoiseConfig tortoiseConfig) {
        super(tortoiseConfig);
    }


    public PullDefaultConsumer(TortoiseConfig tortoiseConfig, Protocol protocol) {
        super(tortoiseConfig, protocol);

    }

    @Override
    protected void doStart() {
        getCore().setRunnable(() -> doSubscription());
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(messageListenerMap.size());
        messageListenerMap.forEach((topic, messageListener) -> {
            scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> topicPullMessage(topic, messageListener), 0, getTortoiseConfig().getPullMessageInterval(), TimeUnit.MILLISECONDS);
        });
    }

    private void topicPullMessage(String topic, MessageListener messageListener) {
        try {
            for (; ; ) {
                MessageGetRequest messageGetRequest = new MessageGetRequest();
                messageGetRequest.setTopic(topic);
                messageGetRequest.setNumber(getTortoiseConfig().getPullMessageNumber());
                MessageGetResponse messageGetResponse = getCore().pullMessage(messageGetRequest);
                Map<Integer, List<MessageEntry>> queueIdMessageMap = messageGetResponse.getQueueIdMessageMap();
                if (queueIdMessageMap.isEmpty()) {
                    LOG.debug("topic : {} , group :{} ,no news has been pulled waiting for the next pull", topic, getTortoiseConfig().getGroup());
                    break;
                }
                queueIdMessageMap.keySet().parallelStream().forEach(queueId -> {
                    List<MessageEntry> messageEntries = queueIdMessageMap.get(queueId);
                    int currentOffset = -1;
                    int commitOffset = -1;
                    try {
                        if (!messageEntries.isEmpty()) {
                            currentOffset = messageEntries.get(0).getOffset();
                        }
                        for (MessageEntry messageEntry : messageEntries) {
                            try {
                                if (messageListener.listener(messageEntry)) {
                                    commitOffset = messageEntry.getNextOffset();
                                } else {
                                    commitOffset = messageEntry.getOffset();
                                    break;
                                }
                            } catch (Exception e) {
                                LOG.error("", e);
                                LOG.error("topic : {} , group :{} ,msg id : {} consumer error", topic, getTortoiseConfig().getGroup(), messageEntry.getId());
                                commitOffset = messageEntry.getOffset();
                                break;
                            }
                        }
                    } finally {
                        if (currentOffset != commitOffset) {
                            OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest();
                            offsetCommitRequest.setTopic(topic);
                            offsetCommitRequest.setQueueId(queueId);
                            offsetCommitRequest.setCurrentOffset(currentOffset);
                            offsetCommitRequest.setCommitOffset(commitOffset);
                            getCore().commitOffset(offsetCommitRequest);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("", e);
            LOG.debug("topic : {} , group :{} ,pull message error", topic, getTortoiseConfig().getGroup());
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
        subscriptionRequest.setGroup(getTortoiseConfig().getGroup());
        subscriptionRequest.setTopics(new HashSet<>(messageListenerMap.keySet()));
        if (!getCore().subscription(subscriptionRequest)) {
            throw new TortoiseException("subscription error");
        }
    }

}
