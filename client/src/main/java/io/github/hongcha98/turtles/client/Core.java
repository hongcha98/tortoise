package io.github.hongcha98.turtles.client;

import io.github.hongcha98.turtles.common.dto.message.MessageAddRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.offset.OffsetCommitRequest;
import io.github.hongcha98.turtles.common.dto.offset.OffsetGetRequest;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionInfoRequest;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionInfoResponse;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionRequest;
import io.github.hongcha98.turtles.common.dto.topic.TopicCreateRequest;

public interface Core extends LifeCycle {
    /**
     * 重新连接之后的操作
     *
     * @param runanber
     */
    void setRunanber(Runnable runanber);

    boolean subscription(SubscriptionRequest subscriptionRequest);

    SubscriptionInfoResponse getSubscriptionInfo(SubscriptionInfoRequest subscriptionInfoRequest);

    boolean createTopic(TopicCreateRequest topicCreateRequest);

    boolean deleteTopic(String topicName);

    /**
     * 返回消息id
     *
     * @param messageAddRequest
     * @return
     */
    String send(MessageAddRequest messageAddRequest);

    /**
     * 获取offset
     */
    int getOffset(OffsetGetRequest offsetGetRequest);

    /**
     * 设置偏移量
     */
    boolean commitOffset(OffsetCommitRequest offsetCommitRequest);

    /**
     * 获取信息
     */
    MessageGetResponse pullMessage(MessageGetRequest messageGetRequest);

}
