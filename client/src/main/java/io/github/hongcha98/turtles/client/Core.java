package io.github.hongcha98.turtles.client;

import io.github.hongcha98.turtles.common.dto.message.request.MessageAddRequest;
import io.github.hongcha98.turtles.common.dto.message.request.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.response.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.offset.request.OffsetCommitRequest;
import io.github.hongcha98.turtles.common.dto.session.request.SubscriptionRequest;
import io.github.hongcha98.turtles.common.dto.topic.request.TopicCreateRequest;

/**
 * 核心api,发送请求到broker,并等待响应
 */
public interface Core extends LifeCycle {
    /**
     * 重新连接之后的操作
     *
     * @param runnable
     */
    void setRunnable(Runnable runnable);

    /**
     * 注册消费主题
     *
     * @param subscriptionRequest
     * @return
     */
    boolean subscription(SubscriptionRequest subscriptionRequest);


    /**
     * 创建主题
     *
     * @param topicCreateRequest
     * @return 是否成功
     */
    boolean createTopic(TopicCreateRequest topicCreateRequest);

    /**
     * 删除主题
     *
     * @param topic
     * @return 是否成功
     */
    boolean deleteTopic(String topic);

    /**
     * 返回消息id
     *
     * @param messageAddRequest
     * @return
     */
    String send(MessageAddRequest messageAddRequest);


    /**
     * 设置偏移量
     */
    boolean commitOffset(OffsetCommitRequest offsetCommitRequest);

    /**
     * 获取信息
     */
    MessageGetResponse pullMessage(MessageGetRequest messageGetRequest);

}
