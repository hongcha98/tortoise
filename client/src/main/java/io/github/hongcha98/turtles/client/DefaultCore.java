package io.github.hongcha98.turtles.client;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.common.exception.RemoteException;
import io.github.hongcha98.remote.core.RemoteClient;
import io.github.hongcha98.remote.core.config.RemoteConfig;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.constant.ProcessConstant;
import io.github.hongcha98.turtles.common.dto.login.LoginRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageAddRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetRequest;
import io.github.hongcha98.turtles.common.dto.message.MessageGetResponse;
import io.github.hongcha98.turtles.common.dto.offset.OffsetCommitRequest;
import io.github.hongcha98.turtles.common.dto.offset.OffsetGetRequest;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionInfoRequest;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionInfoResponse;
import io.github.hongcha98.turtles.common.dto.topic.SubscriptionRequest;
import io.github.hongcha98.turtles.common.dto.topic.TopicCreateRequest;
import io.netty.channel.Channel;


public class DefaultCore implements Core {
    private final TurtlesConfig turtlesConfig;

    private RemoteClient remoteClient;

    private volatile Channel channel;

    private Runnable runnable;

    public DefaultCore(TurtlesConfig turtlesConfig) {
        this.turtlesConfig = turtlesConfig;
    }

    protected <T> T send(Object msg, int code, Class<T> clazz) {
        try {
            Message message = remoteClient.buildRequest(msg, code);
            if (channel == null || !channel.isActive()) {
                synchronized (this) {
                    if (channel == null || !channel.isActive()) {
                        channel = remoteClient.getBootStrap().connect(turtlesConfig.getBrokerHost(), turtlesConfig.getBrokerPort());
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
        loginRequest.setUsername(turtlesConfig.getUsername());
        loginRequest.setPassword(turtlesConfig.getPassword());
        return send(loginRequest, ProcessConstant.PROCESS_LOGIN, Boolean.class);
    }

    @Override
    public void setRunanber(Runnable runanber) {
        this.runnable = runanber;
    }

    @Override
    public boolean subscription(SubscriptionRequest subscriptionRequest) {
        return send(subscriptionRequest, ProcessConstant.PROCESS_SUBSCRIPTION, Boolean.class);
    }

    @Override
    public SubscriptionInfoResponse getSubscriptionInfo(SubscriptionInfoRequest subscriptionInfoRequest) {
        return send(subscriptionInfoRequest, ProcessConstant.PROCESS_SUBSCRIPTION_INFO, SubscriptionInfoResponse.class);
    }

    @Override
    public boolean createTopic(TopicCreateRequest topicCreateRequest) {
        return send(topicCreateRequest, ProcessConstant.PROCESS_TOPIC_CREATE, Boolean.class);
    }

    @Override
    public boolean deleteTopic(String topicName) {
        return send(topicName, ProcessConstant.PROCESS_TOPIC_DELETE, Boolean.class);
    }


    @Override
    public String send(MessageAddRequest message) {
        return send(message, ProcessConstant.PROCESS_MESSAGE_ADD, String.class);
    }

    @Override
    public int getOffset(OffsetGetRequest offsetGetRequest) {
        return send(offsetGetRequest, ProcessConstant.PROCESS_OFFSET_GET, Integer.class);
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
        remoteConfig.setHost(turtlesConfig.getBrokerHost());
        remoteConfig.setPort(turtlesConfig.getBrokerPort());
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
