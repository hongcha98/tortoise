package com.hongcha.hcmq.broker.config;

import com.hongcha.hcmq.broker.queue.Coding;
import com.hongcha.hcmq.broker.queue.DefaultCoding;

import java.io.File;

public class HcmqConfig {
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
    private String storagePath = System.getProperty("user.home") + File.separator + "hcmq";

    /**
     * queue编解码
     *
     * @return
     */
    private Coding coding = new DefaultCoding();

    private String username = "hcmq";

    private String password = "hcmq";

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
