package io.github.hongcha98.tortoise.broker.config;

import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.broker.topic.queue.DefaultCoding;
import io.github.hongcha98.tortoise.broker.topic.queue.Coding;

import java.io.File;

public class TortoiseConfig {
    // broker id
    private int id = 0;
    // 端口
    private int port = 9999;
    // 默认queue数量
    private int queueNumber = 8;
    // 存储位置
    private String storagePath = System.getProperty("user.home") + File.separator + Constant.NAME;
    // message 编解码
    private Coding coding = new DefaultCoding(SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE));
    // 账号
    private String username = Constant.USERNAME;
    // 密码
    private String password = Constant.PASSWORD;
    // 消息保留时间
    private long messageRetentionTime = Constant.MESSAGE_RETENTION_TIME;
    // session task间隔时间
    private long sessionTaskTime = Constant.SESSION_TASK_TIME;
    // 刷盘task间隔时间
    private long brushTaskTime = Constant.BRUSH_TASK_TIME;
    // 消息最大消费次数
    private int consumerLimit = Constant.CONSUMER_LIMIT;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getQueueNumber() {
        return queueNumber;
    }

    public void setQueueNumber(int queueNumber) {
        this.queueNumber = queueNumber;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public Coding getCoding() {
        return coding;
    }

    public void setCoding(Coding coding) {
        this.coding = coding;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getMessageRetentionTime() {
        return messageRetentionTime;
    }

    public void setMessageRetentionTime(long messageRetentionTime) {
        this.messageRetentionTime = messageRetentionTime;
    }

    public long getSessionTaskTime() {
        return sessionTaskTime;
    }

    public void setSessionTaskTime(long sessionTaskTime) {
        this.sessionTaskTime = sessionTaskTime;
    }

    public long getBrushTaskTime() {
        return brushTaskTime;
    }

    public void setBrushTaskTime(long brushTaskTime) {
        this.brushTaskTime = brushTaskTime;
    }

    public int getConsumerLimit() {
        return consumerLimit;
    }

    public void setConsumerLimit(int consumerLimit) {
        this.consumerLimit = consumerLimit;
    }
}
