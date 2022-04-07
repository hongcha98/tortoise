package com.hongcha.turtles.client;

import com.hongcha.turtles.common.dto.message.MessageAddReq;
import com.hongcha.turtles.common.dto.message.MessageGetReq;
import com.hongcha.turtles.common.dto.message.MessageInfo;
import com.hongcha.turtles.common.dto.offset.OffsetCommitReq;
import com.hongcha.turtles.common.dto.offset.OffsetGetReq;
import com.hongcha.turtles.common.dto.topic.GetSubscriptionMessageReq;
import com.hongcha.turtles.common.dto.topic.GetSubscriptionMessageResp;
import com.hongcha.turtles.common.dto.topic.SubscriptionMessageReq;
import com.hongcha.turtles.common.dto.topic.TopicCreateMessageReq;

public interface Core extends LifeCycle {
    /**
     * 重新连接之后的操作
     *
     * @param runanber
     */
    void setRunanber(Runnable runanber);

    boolean subscription(SubscriptionMessageReq subscriptionMessageReq);

    GetSubscriptionMessageResp getSubscriptionInfo(GetSubscriptionMessageReq getSubscriptionMessageReq);

    boolean createTopic(TopicCreateMessageReq topicCreateMessageReq);

    boolean deleteTopic(String topicName);

    /**
     * 返回消息id
     *
     * @param messageAddReq
     * @return
     */
    String send(MessageAddReq messageAddReq);

    /**
     * 获取offset
     */
    int getOffset(OffsetGetReq offsetGetReq);

    /**
     * 设置偏移量
     */
    boolean commitOffset(OffsetCommitReq offsetCommitReq);

    /**
     * 获取信息
     */
    MessageInfo getMessageInfo(MessageGetReq messageGetReq);

}
