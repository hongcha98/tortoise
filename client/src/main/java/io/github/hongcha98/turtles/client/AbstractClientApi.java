package io.github.hongcha98.turtles.client;

import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.topic.TopicCreateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractClientApi implements ClientApi {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());

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
        TopicCreateRequest topicCreateRequest = new TopicCreateRequest();
        topicCreateRequest.setTopicName(topic);
        topicCreateRequest.setQueueNumber(queueNumber);
        return core.createTopic(topicCreateRequest);
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
                doStart();
            } catch (Exception e) {
                close();
                throw new IllegalStateException("start error", e);
            }
        }
    }

    protected void doStart() {

    }


    @Override
    public void close() {
        if (start.compareAndSet(false, true)) {
            core.close();
            doClose();
        }
    }

    protected void doClose() {

    }

    public TurtlesConfig getTurtlesConfig() {
        return turtlesConfig;
    }

    public Core getCore() {
        return core;
    }
}
