package com.hongcha.turtles.broker.constant;

import com.hongcha.turtles.broker.config.TurtlesConfig;
import com.hongcha.turtles.broker.queue.DefaultCoding;
import com.hongcha.remote.core.RemoteClient;
import com.hongcha.remote.core.config.RemoteConfig;

import java.io.File;

public class ConstantTest {
    public static final String PATH = "D://turtles";

    public static final String QUEUE_FILE_NAME = PATH + File.separator + "queue" + Constant.FILE_NAME_SUFFIX;

    public static final String OFFSET_FILE_NAME = PATH + File.separator + "offset" + Constant.FILE_NAME_SUFFIX;

    public static final String TOPIC_NAME = "test-topic";

    public static final String GROUP = "TEST-GROUP";

    public static final TurtlesConfig TURTLES_CONFIG;

    public static final RemoteConfig CLIENT_REMOTE_CONFIG;

    public static final RemoteClient REMOTE_CLIENT;

    static {
        TURTLES_CONFIG = new TurtlesConfig();
        TURTLES_CONFIG.setCoding(new DefaultCoding());
        TURTLES_CONFIG.setStoragePath(PATH);
        CLIENT_REMOTE_CONFIG = new RemoteConfig();
        CLIENT_REMOTE_CONFIG.setPort(TURTLES_CONFIG.getPort());
        REMOTE_CLIENT = new RemoteClient(CLIENT_REMOTE_CONFIG);
    }

}
