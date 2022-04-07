package com.hongcha.hcmq.broker;

import com.hongcha.hcmq.broker.config.HcmqConfig;
import com.hongcha.hcmq.broker.context.ChannelContextManage;
import com.hongcha.hcmq.broker.context.DefaultChannelContextManage;
import com.hongcha.hcmq.broker.error.HcmqException;
import com.hongcha.hcmq.broker.offset.FileOffsetManage;
import com.hongcha.hcmq.broker.offset.OffsetManage;
import com.hongcha.hcmq.broker.process.LoginProcess;
import com.hongcha.hcmq.broker.process.message.MessageAddProcess;
import com.hongcha.hcmq.broker.process.message.MessageGetProcess;
import com.hongcha.hcmq.broker.process.offset.GetOffsetProcess;
import com.hongcha.hcmq.broker.process.offset.OffsetCommitProcess;
import com.hongcha.hcmq.broker.process.topic.*;
import com.hongcha.hcmq.broker.session.DefaultSessionManage;
import com.hongcha.hcmq.broker.session.SessionManage;
import com.hongcha.hcmq.broker.task.TopicIndexRepairTask;
import com.hongcha.hcmq.broker.topic.DefaultTopicManage;
import com.hongcha.hcmq.broker.topic.TopicManage;
import com.hongcha.remote.core.RemoteServer;
import com.hongcha.remote.core.config.RemoteConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hongcha.hcmq.broker.constant.Constant.OFFSET_FILE_NAME;
import static com.hongcha.hcmq.common.dto.constant.ProcessConstant.*;


public class Broker implements LifeCycle {
    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final HcmqConfig hcmqConfig;

    private RemoteServer remoteServer;

    private TopicManage topicManage;

    private OffsetManage offsetManage;

    private ChannelContextManage channelContextManage;

    private SessionManage sessionManage;

    private AtomicBoolean start = new AtomicBoolean(false);

    public Broker(HcmqConfig hcmqConfig) {
        this.hcmqConfig = hcmqConfig;
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                init();
                doStart();
            } catch (Exception e) {
                throw new HcmqException("broker start error", e);
            }

        }
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            try {
                remoteServer.close();
            } catch (Exception e) {
                log.error("remote close error", e);
            }
            topicManage.close();
            offsetManage.close();
        }
    }


    public SessionManage getSubscriptionManage() {
        return sessionManage;
    }

    protected void init() {
        initTopic();
        initOffsetManage();
        sessionManage = new DefaultSessionManage(topicManage);
        initRemoteServer();
    }

    private void initOffsetManage() {
        offsetManage = new FileOffsetManage(new File(hcmqConfig.getStoragePath(), OFFSET_FILE_NAME), topicManage);
    }

    protected void initTopic() {
        topicManage = new DefaultTopicManage(hcmqConfig);
        topicManage.start();
    }

    private void initRemoteServer() {
        channelContextManage = new DefaultChannelContextManage();
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setPort(hcmqConfig.getPort());
        remoteServer = new RemoteServer(remoteConfig);
        registryProcess();
        try {
            remoteServer.start();
        } catch (Exception e) {
            throw new HcmqException("remote server start error", e);
        }

    }

    /**
     * 注册处理器
     */
    protected void registryProcess() {
        ExecutorService subscriptionExecutors = Executors.newSingleThreadExecutor();
        ExecutorService offsetExecutors = Executors.newSingleThreadExecutor();
        ExecutorService messageExecutorService = Executors.newSingleThreadExecutor();
        ExecutorService topicExecutorService = Executors.newSingleThreadExecutor();

        remoteServer.registerProcess(PROCESS_LOGIN, new LoginProcess(this), Executors.newSingleThreadExecutor());
        remoteServer.registerProcess(PROCESS_SUBSCRIPTION, new SubscriptionProcess(this), subscriptionExecutors);
        remoteServer.registerProcess(PROCESS_UNSUBSCRIPTION, new UnSubscriptionProcess(this), subscriptionExecutors);
        remoteServer.registerProcess(PROCESS_GET_SUBSCRIPTION, new GetSubscriptionMessageProcess(this), subscriptionExecutors);
        remoteServer.registerProcess(PROCESS_GET_OFFSET, new GetOffsetProcess(this), offsetExecutors);
        remoteServer.registerProcess(PROCESS_GET_MESSAGE, new MessageGetProcess(this), messageExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_MESSAGE_ADD, new MessageAddProcess(this), messageExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_CREATE, new TopicCreateProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_DELETE, new TopicDeleteProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_COMMIT_OFFSET, new OffsetCommitProcess(this), offsetExecutors);


    }

    protected void doStart() {
        new Thread(new TopicIndexRepairTask(this)).start();
    }

    public HcmqConfig getHcmqConfig() {
        return hcmqConfig;
    }


    public RemoteServer getRemoteServer() {
        return remoteServer;
    }


    public TopicManage getTopicManage() {
        return topicManage;
    }


    public OffsetManage getOffsetManage() {
        return offsetManage;
    }


    public ChannelContextManage getChannelContextManage() {
        return channelContextManage;
    }
}
