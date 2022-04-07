package com.hongcha.hcmq.clent;

import com.hongcha.hcmq.clent.config.HcmqConfig;
import com.hongcha.hcmq.common.dto.constant.ProcessConstant;
import com.hongcha.hcmq.common.dto.login.LoginMessageReq;
import com.hongcha.hcmq.common.dto.message.MessageAddReq;
import com.hongcha.hcmq.common.dto.message.MessageGetReq;
import com.hongcha.hcmq.common.dto.message.MessageInfo;
import com.hongcha.hcmq.common.dto.offset.OffsetCommitReq;
import com.hongcha.hcmq.common.dto.offset.OffsetGetReq;
import com.hongcha.hcmq.common.dto.topic.GetSubscriptionMessageReq;
import com.hongcha.hcmq.common.dto.topic.GetSubscriptionMessageResp;
import com.hongcha.hcmq.common.dto.topic.SubscriptionMessageReq;
import com.hongcha.hcmq.common.dto.topic.TopicCreateMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.common.exception.RemoteException;
import com.hongcha.remote.core.RemoteClient;
import com.hongcha.remote.core.config.RemoteConfig;
import io.netty.channel.Channel;


public class DefaultCore implements Core {
    private final HcmqConfig hcmqConfig;

    private RemoteClient remoteClient;

    private Channel channel;

    private Runnable runnable;

    public DefaultCore(HcmqConfig hcmqConfig) {
        this.hcmqConfig = hcmqConfig;
    }

    protected <T> T send(Object msg, int code, Class<T> clazz) {
        try {
            Message message = remoteClient.buildRequest(msg, code);
            if (channel == null || !channel.isActive()) {
                channel = remoteClient.getBootStrap().connect(hcmqConfig.getBrokerHost(), hcmqConfig.getBrokerPort());
                if (!login()) {
                    throw new IllegalStateException("login error");
                }
                if (runnable != null) {
                    runnable.run();
                }
            }
            return remoteClient.send(channel, message, clazz);
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }


    protected boolean login() {
        LoginMessageReq loginMessageReq = new LoginMessageReq();
        loginMessageReq.setUsername(hcmqConfig.getUsername());
        loginMessageReq.setPassword(hcmqConfig.getPassword());
        return send(loginMessageReq, ProcessConstant.PROCESS_LOGIN, Boolean.class);
    }

    @Override
    public void setRunanber(Runnable runanber) {
        this.runnable = runanber;
    }

    @Override
    public boolean subscription(SubscriptionMessageReq subscriptionMessageReq) {
        return send(subscriptionMessageReq, ProcessConstant.PROCESS_SUBSCRIPTION, Boolean.class);
    }

    @Override
    public GetSubscriptionMessageResp getSubscriptionInfo(GetSubscriptionMessageReq getSubscriptionMessageReq) {
        return send(getSubscriptionMessageReq, ProcessConstant.PROCESS_GET_SUBSCRIPTION, GetSubscriptionMessageResp.class);
    }

    @Override
    public boolean createTopic(TopicCreateMessageReq topicCreateMessageReq) {
        return send(topicCreateMessageReq, ProcessConstant.PROCESS_TOPIC_CREATE, Boolean.class);
    }

    @Override
    public boolean deleteTopic(String topicName) {
        return send(topicName, ProcessConstant.PROCESS_TOPIC_DELETE, Boolean.class);
    }


    @Override
    public String send(MessageAddReq message) {
        return send(message, ProcessConstant.PROCESS_TOPIC_MESSAGE_ADD, String.class);
    }

    @Override
    public int getOffset(OffsetGetReq offsetGetReq) {
        return send(offsetGetReq, ProcessConstant.PROCESS_GET_OFFSET, Integer.class);
    }

    @Override
    public boolean commitOffset(OffsetCommitReq offsetCommitReq) {
        return send(offsetCommitReq, ProcessConstant.PROCESS_COMMIT_OFFSET, Boolean.class);
    }

    @Override
    public MessageInfo getMessageInfo(MessageGetReq messageGetReq) {
        return send(messageGetReq, ProcessConstant.PROCESS_GET_MESSAGE, MessageInfo.class);
    }

    @Override
    public void start() {
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setHost(hcmqConfig.getBrokerHost());
        remoteConfig.setPort(hcmqConfig.getBrokerPort());
        remoteClient = new RemoteClient(remoteConfig);
        try {
            remoteClient.start();
        } catch (Exception e) {
            //TODO
        }

    }

    @Override
    public void close() {
        try {
            remoteClient.close();
        } catch (Exception e) {
            //TODO
        }
    }
}
