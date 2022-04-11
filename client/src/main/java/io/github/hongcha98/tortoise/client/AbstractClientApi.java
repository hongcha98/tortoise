package io.github.hongcha98.tortoise.client;

import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.client.config.TortoiseConfig;
import io.github.hongcha98.tortoise.common.dto.topic.request.TopicCreateRequest;
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

    private final TortoiseConfig tortoiseConfig;

    private final Protocol protocol;

    private Core core;

    private ExecutorService sendExecutorService;


    public AbstractClientApi(TortoiseConfig tortoiseConfig) {
        this(tortoiseConfig, SpiLoader.load(Protocol.class, PROTOCOL_DEFAULT_NAME));
        checkConfig(tortoiseConfig);

    }

    public AbstractClientApi(TortoiseConfig tortoiseConfig, Protocol protocol) {
        this.tortoiseConfig = tortoiseConfig;
        this.protocol = protocol;
    }

    protected void checkConfig(TortoiseConfig tortoiseConfig) {
        String group = tortoiseConfig.getGroup();
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
                core = new DefaultCore(tortoiseConfig);
                sendExecutorService = Executors.newFixedThreadPool(getTortoiseConfig().getSendThreadNum());
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

    public TortoiseConfig getTortoiseConfig() {
        return tortoiseConfig;
    }

    public Core getCore() {
        return core;
    }

    public AtomicBoolean getStart() {
        return start;
    }
}
