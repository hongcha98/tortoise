package io.github.hongcha98.tortoise.broker.constant;

import io.github.hongcha98.remote.common.constant.RemoteConstant;

import java.util.concurrent.TimeUnit;

public class Constant {
    // 名称
    public static final String NAME = "tortoise";
    // queue文件扩容大小
    public static final int QUEUE_FILE_ADD_SIZE = 1024 * 1024 * 10;
    // 达到文件的占比就扩容
    public static final double QUEUE_FILE_SIZE_EXPANSION_PERCENTAGE = 0.75;
    // 协议
    public static final int PROTOCOL_CODE = RemoteConstant.DEFAULT_PROTOCOL;
    // 文件后缀名
    public static final String FILE_NAME_SUFFIX = "." + NAME;
    // offset文件名称
    public static final String OFFSET_FILE_NAME = "offset" + FILE_NAME_SUFFIX;
    // 文件长度记录的index
    public static final int FILE_LENGTH_INDEX = 0;
    // 文件长度记录占的字节大小
    public static final int FILE_LENGTH = 4;
    // 消息长度记录占的字节大小
    public static final int MESSAGE_LENGTH = 4;
    // 消息消费次数占的字节大小
    public static final int CONSUMER_NUMBER_LENGTH = 1;
    // 消息元数据占用字节大小
    public static final int MESSAGE_METADATA_LENGTH = MESSAGE_LENGTH + CONSUMER_NUMBER_LENGTH;
    // 账号
    public static final String USERNAME = NAME;
    // 密码
    public static final String PASSWORD = NAME;
    // 消息保留时间
    public static final long MESSAGE_RETENTION_TIME = TimeUnit.DAYS.toMillis(3);
    // queue文件尝试获取锁的时间
    public static final long QUEUE_FILE_TRY_LOCK_TIME = 500;
    // 刷磁盘时间间隔
    public static final long BRUSH_TASK_TIME = 5000;
    // session task time
    public static final long SESSION_TASK_TIME = 200;
    // 消费次数限额
    public static final int CONSUMER_LIMIT = 16;
}
