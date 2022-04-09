package io.github.hongcha98.turtles.broker;

import io.github.hongcha98.remote.core.RemoteServer;
import io.github.hongcha98.remote.core.config.RemoteConfig;
import io.github.hongcha98.turtles.broker.config.TurtlesConfig;
import io.github.hongcha98.turtles.broker.constant.Constant;
import io.github.hongcha98.turtles.broker.context.ChannelContextManage;
import io.github.hongcha98.turtles.broker.context.DefaultChannelContextManage;
import io.github.hongcha98.turtles.broker.error.TurtlesException;
import io.github.hongcha98.turtles.broker.offset.FileOffsetManage;
import io.github.hongcha98.turtles.broker.offset.OffsetManage;
import io.github.hongcha98.turtles.broker.process.LoginProcess;
import io.github.hongcha98.turtles.broker.process.message.MessageAddProcess;
import io.github.hongcha98.turtles.broker.process.message.MessageGetProcess;
import io.github.hongcha98.turtles.broker.process.offset.GetOffsetProcess;
import io.github.hongcha98.turtles.broker.process.offset.OffsetCommitProcess;
import io.github.hongcha98.turtles.broker.process.topic.*;
import io.github.hongcha98.turtles.broker.session.DefaultSessionManage;
import io.github.hongcha98.turtles.broker.session.SessionManage;
import io.github.hongcha98.turtles.broker.task.SessionTask;
import io.github.hongcha98.turtles.broker.topic.DefaultTopicManage;
import io.github.hongcha98.turtles.broker.topic.TopicManage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.hongcha98.turtles.common.dto.constant.ProcessConstant.*;


public class TurtlesBroker implements LifeCycle {
    private static final Logger log = LoggerFactory.getLogger(TurtlesBroker.class);

    private final TurtlesConfig turtlesConfig;

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
            sessionExecutorService.shutdown();
            messageExecutorService.shutdown();
            offsetExecutorService.shutdown();
            offsetExecutorService.shutdown();
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
        offsetManage = new FileOffsetManage(new File(turtlesConfig.getStoragePath(), Constant.OFFSET_FILE_NAME), topicManage);
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
        /**
         * session相关
         */
        remoteServer.registerProcess(PROCESS_LOGIN, new LoginProcess(this), sessionExecutorService);
        remoteServer.registerProcess(PROCESS_SUBSCRIPTION, new SubscriptionProcess(this), sessionExecutorService);
        remoteServer.registerProcess(PROCESS_UNSUBSCRIPTION, new UnSubscriptionProcess(this), sessionExecutorService);
        remoteServer.registerProcess(PROCESS_SUBSCRIPTION_INFO, new GetSubscriptionMessageProcess(this), sessionExecutorService);

        /**
         * message相关
         */

        remoteServer.registerProcess(PROCESS_MESSAGE_SESSION_PULL, new MessageGetProcess(this), messageExecutorService);
        remoteServer.registerProcess(PROCESS_MESSAGE_ADD, new MessageAddProcess(this), messageExecutorService);

        /**
         * offset 相关
         */

        remoteServer.registerProcess(PROCESS_OFFSET_GET, new GetOffsetProcess(this), offsetExecutorService);
        remoteServer.registerProcess(PROCESS_OFFSET_COMMIT, new OffsetCommitProcess(this), offsetExecutorService);

        /**
         * topic相关
         */

        remoteServer.registerProcess(PROCESS_TOPIC_CREATE, new TopicCreateProcess(this), topicExecutorService);
        remoteServer.registerProcess(PROCESS_TOPIC_DELETE, new TopicDeleteProcess(this), topicExecutorService);

    }

    protected void doStart() {
        taskExecutorService.scheduleAtFixedRate(new SessionTask(this), 200, 200, TimeUnit.MILLISECONDS);
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
