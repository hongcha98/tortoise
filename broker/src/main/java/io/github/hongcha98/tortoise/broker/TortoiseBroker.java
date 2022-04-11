package io.github.hongcha98.tortoise.broker;

import io.github.hongcha98.remote.core.RemoteServer;
import io.github.hongcha98.remote.core.config.RemoteConfig;
import io.github.hongcha98.tortoise.broker.config.TortoiseConfig;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.broker.context.ChannelContextManage;
import io.github.hongcha98.tortoise.broker.context.DefaultChannelContextManage;
import io.github.hongcha98.tortoise.broker.offset.FileOffsetManage;
import io.github.hongcha98.tortoise.broker.offset.OffsetManage;
import io.github.hongcha98.tortoise.broker.process.message.MessageAddProcess;
import io.github.hongcha98.tortoise.broker.process.message.MessageGetProcess;
import io.github.hongcha98.tortoise.broker.process.offset.OffsetCommitProcess;
import io.github.hongcha98.tortoise.broker.process.session.LoginProcess;
import io.github.hongcha98.tortoise.broker.process.session.SubscriptionProcess;
import io.github.hongcha98.tortoise.broker.process.session.UnSubscriptionProcess;
import io.github.hongcha98.tortoise.broker.process.topic.TopicCreateProcess;
import io.github.hongcha98.tortoise.broker.process.topic.TopicDeleteProcess;
import io.github.hongcha98.tortoise.broker.session.DefaultSessionManage;
import io.github.hongcha98.tortoise.broker.session.SessionManage;
import io.github.hongcha98.tortoise.broker.task.SessionTask;
import io.github.hongcha98.tortoise.broker.task.TopicBrushTask;
import io.github.hongcha98.tortoise.broker.topic.DefaultTopicManage;
import io.github.hongcha98.tortoise.broker.topic.TopicManage;
import io.github.hongcha98.tortoise.common.error.TortoiseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.hongcha98.tortoise.common.dto.constant.ProcessConstant.*;


public class TortoiseBroker implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(TortoiseBroker.class);

    private final TortoiseConfig tortoiseConfig;

    private RemoteServer remoteServer;

    private TopicManage topicManage;

    private OffsetManage offsetManage;

    private ChannelContextManage channelContextManage;

    private SessionManage sessionManage;

    private AtomicBoolean start = new AtomicBoolean(false);

    private ExecutorService sessionExecutorService;
    private ExecutorService messageExecutorService;
    private ExecutorService offsetExecutorService;
    private ExecutorService topicExecutorService;
    private ScheduledExecutorService taskExecutorService;

    public TortoiseBroker(TortoiseConfig tortoiseConfig) {
        this.tortoiseConfig = tortoiseConfig;
    }

    @Override
    public void start() {
        if (start.compareAndSet(false, true)) {
            try {
                init();
                doStart();
            } catch (Exception e) {
                throw new TortoiseException("broker start error", e);
            }

        }
    }

    @Override
    public void close() {
        if (start.compareAndSet(true, false)) {
            try {
                remoteServer.close();
            } catch (Exception e) {
                LOG.error("remote close error", e);
            }
            topicManage.close();
            offsetManage.close();
            sessionExecutorService.shutdown();
            messageExecutorService.shutdown();
            offsetExecutorService.shutdown();
            topicExecutorService.shutdown();
            taskExecutorService.shutdown();
        }
    }


    public SessionManage getSessionManage() {
        return sessionManage;
    }

    protected void init() {
        initTopic();
        initOffsetManage();
        sessionManage = new DefaultSessionManage(topicManage);
        initExecutors();
        initRemoteServer();
    }

    private void initExecutors() {
        // channel session处理 1个线程就足够了
        sessionExecutorService = Executors.newSingleThreadExecutor(new DefaultThreadFactory("sessionExecutorService"));
        // message线程池要大一点,要写入数据和读取数据
        messageExecutorService = Executors.newFixedThreadPool(32, new DefaultThreadFactory("messageExecutorService"));
        offsetExecutorService = Executors.newSingleThreadExecutor(new DefaultThreadFactory("offsetExecutorService"));
        topicExecutorService = Executors.newSingleThreadExecutor(new DefaultThreadFactory("topicExecutorService"));
        taskExecutorService = new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("taskExecutorService"));
    }

    private void initOffsetManage() {
        offsetManage = new FileOffsetManage(new File(tortoiseConfig.getStoragePath(), Constant.OFFSET_FILE_NAME), topicManage);
        offsetManage.start();
    }

    protected void initTopic() {
        topicManage = new DefaultTopicManage(tortoiseConfig);
        topicManage.start();
    }

    private void initRemoteServer() {
        channelContextManage = new DefaultChannelContextManage();
        RemoteConfig remoteConfig = new RemoteConfig();
        remoteConfig.setPort(tortoiseConfig.getPort());
        remoteServer = new RemoteServer(remoteConfig);
        registryProcess();
        try {
            remoteServer.start();
        } catch (Exception e) {
            throw new TortoiseException("remote server start error", e);
        }

    }

    /**
     * 注册处理器
     */
    protected void registryProcess() {
        /**
         * session相关
         */
        remoteServer.registerProcess(PROCESS_LOGIN, new LoginProcess(this), sessionExecutorService);
        remoteServer.registerProcess(PROCESS_SUBSCRIPTION, new SubscriptionProcess(this), sessionExecutorService);
        remoteServer.registerProcess(PROCESS_UNSUBSCRIPTION, new UnSubscriptionProcess(this), sessionExecutorService);

        /**
         * message相关
         */
        remoteServer.registerProcess(PROCESS_MESSAGE_SESSION_PULL, new MessageGetProcess(this), messageExecutorService);
        remoteServer.registerProcess(PROCESS_MESSAGE_ADD, new MessageAddProcess(this), messageExecutorService);

        /**
         * offset 相关
         */
        remoteServer.registerProcess(PROCESS_OFFSET_COMMIT, new OffsetCommitProcess(this), offsetExecutorService);

        /**
         * topic相关
         */
        remoteServer.registerProcess(PROCESS_TOPIC_CREATE, new TopicCreateProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_DELETE, new TopicDeleteProcess(this), topicExecutorService);

    }

    protected void doStart() {
        taskExecutorService.scheduleAtFixedRate(new SessionTask(this), tortoiseConfig.getSessionTaskTime(), tortoiseConfig.getSessionTaskTime(), TimeUnit.MILLISECONDS);
        taskExecutorService.scheduleAtFixedRate(new TopicBrushTask(this), tortoiseConfig.getBrushTaskTime(), tortoiseConfig.getBrushTaskTime(), TimeUnit.MILLISECONDS);
    }

    public TortoiseConfig getTortoiseConfig() {
        return tortoiseConfig;
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
