package com.hongcha.turtles.broker;

import com.hongcha.remote.core.RemoteServer;
import com.hongcha.remote.core.config.RemoteConfig;
import com.hongcha.turtles.broker.config.TurtlesConfig;
import com.hongcha.turtles.broker.context.ChannelContextManage;
import com.hongcha.turtles.broker.context.DefaultChannelContextManage;
import com.hongcha.turtles.broker.error.TurtlesException;
import com.hongcha.turtles.broker.offset.FileOffsetManage;
import com.hongcha.turtles.broker.offset.OffsetManage;
import com.hongcha.turtles.broker.process.LoginProcess;
import com.hongcha.turtles.broker.process.message.MessageAddProcess;
import com.hongcha.turtles.broker.process.message.MessageGetProcess;
import com.hongcha.turtles.broker.process.offset.GetOffsetProcess;
import com.hongcha.turtles.broker.process.offset.OffsetCommitProcess;
import com.hongcha.turtles.broker.process.topic.*;
import com.hongcha.turtles.broker.session.DefaultSessionManage;
import com.hongcha.turtles.broker.session.SessionManage;
import com.hongcha.turtles.broker.task.TopicIndexRepairTask;
import com.hongcha.turtles.broker.topic.DefaultTopicManage;
import com.hongcha.turtles.broker.topic.TopicManage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hongcha.turtles.broker.constant.Constant.OFFSET_FILE_NAME;
import static com.hongcha.turtles.common.dto.constant.ProcessConstant.*;


public class TurtlesBroker implements LifeCycle {
    private static final Logger log = LoggerFactory.getLogger(TurtlesBroker.class);

    private final TurtlesConfig turtlesConfig;

    private RemoteServer remoteServer;

    private TopicManage topicManage;

    private OffsetManage offsetManage;

    private ChannelContextManage channelContextManage;

    private SessionManage sessionManage;

    private AtomicBoolean start = new AtomicBoolean(false);

    public TurtlesBroker(TurtlesConfig turtlesConfig) {
        this.turtlesConfig = turtlesConfig;
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                init();
                doStart();
            } catch (Exception e) {
                throw new TurtlesException("broker start error", e);
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


    public SessionManage getSessionManage() {
        return sessionManage;
    }

    protected void init() {
        initTopic();
        initOffsetManage();
        sessionManage = new DefaultSessionManage(topicManage);
        initRemoteServer();
    }

    private void initOffsetManage() {
        offsetManage = new FileOffsetManage(new File(turtlesConfig.getStoragePath(), OFFSET_FILE_NAME), topicManage);
        offsetManage.start();
    }

    protected void initTopic() {
        topicManage = new DefaultTopicManage(turtlesConfig);
        topicManage.start();
    }

    private void initRemoteServer() {
        channelContextManage = new DefaultChannelContextManage();
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setPort(turtlesConfig.getPort());
        remoteServer = new RemoteServer(remoteConfig);
        registryProcess();
        try {
            remoteServer.start();
        } catch (Exception e) {
            throw new TurtlesException("remote server start error", e);
        }

    }

    /**
     * 注册处理器
     */
    protected void registryProcess() {
        // channel session处理 1个线程就足够了
        ExecutorService sessionExecutors = Executors.newSingleThreadExecutor(new DefaultThreadFactory("sessionExecutors"));
        remoteServer.registerProcess(PROCESS_LOGIN, new LoginProcess(this), sessionExecutors);
        remoteServer.registerProcess(PROCESS_SUBSCRIPTION, new SubscriptionProcess(this), sessionExecutors);
        remoteServer.registerProcess(PROCESS_UNSUBSCRIPTION, new UnSubscriptionProcess(this), sessionExecutors);
        remoteServer.registerProcess(PROCESS_GET_SUBSCRIPTION, new GetSubscriptionMessageProcess(this), sessionExecutors);

        // message线程池要大一点,要写入数据和读取数据
        ExecutorService messageExecutorService = Executors.newFixedThreadPool(32, new DefaultThreadFactory("messageExecutorService"));
        remoteServer.registerProcess(PROCESS_GET_MESSAGE, new MessageGetProcess(this), messageExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_MESSAGE_ADD, new MessageAddProcess(this), messageExecutorService);


        ExecutorService offsetExecutors = Executors.newSingleThreadExecutor();
        ExecutorService topicExecutorService = Executors.newSingleThreadExecutor();
        remoteServer.registerProcess(PROCESS_GET_OFFSET, new GetOffsetProcess(this), offsetExecutors);

        remoteServer.registerProcess(PROCESS_TOPIC_CREATE, new TopicCreateProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_DELETE, new TopicDeleteProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_COMMIT_OFFSET, new OffsetCommitProcess(this), offsetExecutors);


    }

    protected void doStart() {
        new Thread(new TopicIndexRepairTask(this)).start();
    }

    public TurtlesConfig getTurtlesConfig() {
        return turtlesConfig;
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

    static class DefaultThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
