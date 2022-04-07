package com.hongcha.hcmq.broker.constant;

import com.hongcha.hcmq.broker.config.HcmqConfig;
import com.hongcha.hcmq.broker.queue.DefaultCoding;
import com.hongcha.remote.core.RemoteClient;
import com.hongcha.remote.core.config.RemoteConfig;

import java.io.File;

public class ConstantTest {
    public static final String PATH = "D://hcmq";

    public static final String QUEUE_FILE_NAME = PATH + File.separator + "queue" + Constant.FILE_NAME_SUFFIX;

    public static final String OFFSET_FILE_NAME = PATH + File.separator + "offset" + Constant.FILE_NAME_SUFFIX;

    public static final String TOPIC_NAME = "test-topic";

    public static final String GROUP = "TEST-GROUP";

    public static final HcmqConfig HCMQ_CONFIG;

    public static final RemoteConfig CLIENT_REMOTE_CONFIG;

    public static final RemoteClient REMOTE_CLIENT;

    static {
        HCMQ_CONFIG = new HcmqConfig();
        HCMQ_CONFIG.setCoding(new DefaultCoding());
        HCMQ_CONFIG.setStoragePath(PATH);
        CLIENT_REMOTE_CONFIG = new RemoteConfig();
        CLIENT_REMOTE_CONFIG.setPort(HCMQ_CONFIG.getPort());
        REMOTE_CLIENT = new RemoteClient(CLIENT_REMOTE_CONFIG);
    }

}
