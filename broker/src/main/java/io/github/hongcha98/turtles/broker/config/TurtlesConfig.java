package io.github.hongcha98.turtles.broker.config;

import io.github.hongcha98.turtles.broker.constant.Constant;
import io.github.hongcha98.turtles.broker.topic.queue.Coding;
import io.github.hongcha98.turtles.broker.topic.queue.DefaultCoding;

import java.io.File;

public class TurtlesConfig {
    /**
     * broker id
     */
    private int id = 0;
    /**
     * 端口
     */
    private int port = 9999;
    /**
     * 默认queue数量
     */
    private int queueNumber = 8;
    /**
     * 存储位置
     */
    private String storagePath = System.getProperty("user.home") + File.separator + Constant.NAME;

    /**
     * queue编解码
     *
     * @return
     */
    private Coding coding = new DefaultCoding();

    private String username = Constant.USERNAME;

    private String password = Constant.PASSWORD;

    private long messageRetentionTime = Constant.MESSAGE_RETENTION_TIME;
    private long sessionTime = Constant.SESSION_TIME;
    private long brushTime = Constant.BRUSH_TIME;

    public long getSessionTime() {
        return sessionTime;
    }

    public void setSessionTime(long sessionTime) {
        this.sessionTime = sessionTime;
    }

    public long getBrushTime() {
        return brushTime;
    }

    public void setBrushTime(long brushTime) {
        this.brushTime = brushTime;
    }

    public long getMessageRetentionTime() {
        return messageRetentionTime;
    }

    public void setMessageRetentionTime(long messageRetentionTime) {
        this.messageRetentionTime = messageRetentionTime;
    }

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
}
