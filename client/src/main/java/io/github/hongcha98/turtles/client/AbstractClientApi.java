package io.github.hongcha98.turtles.client;

import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.client.config.TurtlesConfig;
import io.github.hongcha98.turtles.common.dto.topic.request.TopicCreateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class AbstractClientApi implements ClientApi {
    // 默认使用json编解码
    private static final String PROTOCOL_DEFAULT_NAME = "json";

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final AtomicBoolean start = new AtomicBoolean(false);

    private final TurtlesConfig turtlesConfig;

    private final Protocol protocol;

    private Core core;

    private ExecutorService sendExecutorService;


    public AbstractClientApi(TurtlesConfig turtlesConfig) {
        this(turtlesConfig, SpiLoader.load(Protocol.class, PROTOCOL_DEFAULT_NAME));
        checkConfig(turtlesConfig);

    }

    public AbstractClientApi(TurtlesConfig turtlesConfig, Protocol protocol) {
        this.turtlesConfig = turtlesConfig;
        this.protocol = protocol;
    }

    protected void checkConfig(TurtlesConfig turtlesConfig) {
        String group = turtlesConfig.getGroup();
        if (group == null || group.trim().isEmpty()) {
            throw new IllegalStateException("please configure group name");
        }
    }

    public Protocol getProtocol() {
        return protocol;
    }

    @Override
    public boolean createTopic(String topic, int queueNumber) {
        TopicCreateRequest topicCreateRequest = new TopicCreateRequest();
        topicCreateRequest.setTopic(topic);
        topicCreateRequest.setQueueNumber(queueNumber);
        return core.createTopic(topicCreateRequest);
    }

    @Override
    public boolean deleteTopic(String topic) {
        return core.deleteTopic(topic);
    }

    protected <T> CompletableFuture<T> asyncSend(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, sendExecutorService);
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                core = new DefaultCore(turtlesConfig);
                sendExecutorService = Executors.newFixedThreadPool(getTurtlesConfig().getSendThreadNum());
                core.start();
                doStart();
            } catch (Exception e) {
                throw new IllegalStateException("start error", e);
            }
        }
    }

    protected void doStart() {

    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            sendExecutorService.shutdown();
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

    public AtomicBoolean getStart() {
        return start;
    }
}
