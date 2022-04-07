package com.hongcha.turtles.client;

import com.hongcha.turtles.client.config.TurtlesConfig;
import com.hongcha.turtles.common.dto.login.LoginMessageReq;
import com.hongcha.turtles.common.dto.topic.TopicCreateMessageReq;
import com.hongcha.remote.common.spi.SpiLoader;
import com.hongcha.remote.protocol.Protocol;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractClientApi implements ClientApi {
    private final AtomicBoolean start = new AtomicBoolean(false);

    private final TurtlesConfig turtlesConfig;

    private final Protocol protocol;

    private final Core core;

    public AbstractClientApi(TurtlesConfig turtlesConfig) {
        this(turtlesConfig, SpiLoader.load(Protocol.class, 2));
        checkConfig(turtlesConfig);

    }

    public AbstractClientApi(TurtlesConfig turtlesConfig, Protocol protocol) {
        this.turtlesConfig = turtlesConfig;
        this.protocol = protocol;
        this.core = new DefaultCore(turtlesConfig);
    }

    protected void checkConfig(TurtlesConfig turtlesConfig) {
        String groupName = turtlesConfig.getGroupName();
        if (groupName == null || groupName.trim().isEmpty()) {
            throw new IllegalStateException("please configure group name");
        }
    }

    public Protocol getProtocol() {
        return protocol;
    }

    @Override
    public boolean createTopic(String topic, int queueNumber) {
        TopicCreateMessageReq topicCreateMessageReq = new TopicCreateMessageReq();
        topicCreateMessageReq.setTopicName(topic);
        topicCreateMessageReq.setQueueNumber(queueNumber);
        return core.createTopic(topicCreateMessageReq);
    }

    @Override
    public boolean deleteTopic(String topic) {
        return core.deleteTopic(topic);
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                core.start();
                LoginMessageReq loginMessageReq = new LoginMessageReq();
                loginMessageReq.setUsername(turtlesConfig.getUsername());
                loginMessageReq.setPassword(turtlesConfig.getPassword());
                doStart();
            } catch (Exception e) {
                close();
                throw new IllegalStateException("start error", e);
            }
        }
    }

    protected abstract void doStart() throws Exception;

    @Override
    public void close() {
        if (start.compareAndSet(false, true)) {
            core.close();
            doClose();
        }
    }

    protected abstract void doClose();

    public TurtlesConfig TurtlesConfig() {
        return turtlesConfig;
    }

    public Core getCore() {
        return core;
    }
}
