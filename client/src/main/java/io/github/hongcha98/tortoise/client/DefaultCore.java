package io.github.hongcha98.tortoise.client;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.common.exception.RemoteException;
import io.github.hongcha98.remote.core.RemoteClient;
import io.github.hongcha98.remote.core.config.RemoteConfig;
import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.constant.ProcessConstant;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageAddRequest;
import io.github.hongcha98.tortoise.common.dto.message.request.MessageGetRequest;
import io.github.hongcha98.tortoise.common.dto.message.response.MessageGetResponse;
import io.github.hongcha98.tortoise.common.dto.offset.request.OffsetCommitRequest;
import io.github.hongcha98.tortoise.common.dto.session.request.LoginRequest;
import io.github.hongcha98.tortoise.common.dto.session.request.SubscriptionRequest;
import io.github.hongcha98.tortoise.common.dto.topic.request.TopicCreateRequest;
import io.netty.channel.Channel;


public class DefaultCore implements Core {
    private final TortoiseConfig tortoiseConfig;

    private RemoteClient remoteClient;

    private volatile Channel channel;

    private Runnable runnable;

    public DefaultCore(TortoiseConfig tortoiseConfig) {
        this.tortoiseConfig = tortoiseConfig;
    }

    protected <T> T send(Object msg, int code, Class<T> clazz) {
        try {
            Message message = remoteClient.buildRequest(msg, code);
            if (channel == null || !channel.isActive()) {
                synchronized (this) {
                    if (channel == null || !channel.isActive()) {
                        channel = remoteClient.getBootStrap().connect(tortoiseConfig.getBrokerHost(), tortoiseConfig.getBrokerPort());
                        if (!login()) {
                            throw new IllegalStateException("login error");
                        }
                        if (runnable != null) {
                            runnable.run();
                        }
                    }
                }
            }
            return remoteClient.send(channel, message, clazz);
        } catch (Exception e) {
            throw new RemoteException(e);
        }
    }


    protected boolean login() {
        LoginRequest loginRequest = new LoginRequest();
        loginRequest.setUsername(tortoiseConfig.getUsername());
        loginRequest.setPassword(tortoiseConfig.getPassword());
        return send(loginRequest, ProcessConstant.PROCESS_LOGIN, Boolean.class);
    }

    @Override
    public void setRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public boolean subscription(SubscriptionRequest subscriptionRequest) {
        return send(subscriptionRequest, ProcessConstant.PROCESS_SUBSCRIPTION, Boolean.class);
    }


    @Override
    public boolean createTopic(TopicCreateRequest topicCreateRequest) {
        return send(topicCreateRequest, ProcessConstant.PROCESS_TOPIC_CREATE, Boolean.class);
    }

    @Override
    public boolean deleteTopic(String topic) {
        return send(topic, ProcessConstant.PROCESS_TOPIC_DELETE, Boolean.class);
    }


    @Override
    public String send(MessageAddRequest message) {
        return send(message, ProcessConstant.PROCESS_MESSAGE_ADD, String.class);
    }


    @Override
    public boolean commitOffset(OffsetCommitRequest offsetCommitRequest) {
        return send(offsetCommitRequest, ProcessConstant.PROCESS_OFFSET_COMMIT, Boolean.class);
    }

    @Override
    public MessageGetResponse pullMessage(MessageGetRequest messageGetRequest) {
        return send(messageGetRequest, ProcessConstant.PROCESS_MESSAGE_SESSION_PULL, MessageGetResponse.class);
    }

    @Override
    public void start() {
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setHost(tortoiseConfig.getBrokerHost());
        remoteConfig.setPort(tortoiseConfig.getBrokerPort());
        remoteClient = new RemoteClient(remoteConfig);
        remoteClient.start();
    }

    @Override
    public void close() {
        remoteClient.close();
    }
}
