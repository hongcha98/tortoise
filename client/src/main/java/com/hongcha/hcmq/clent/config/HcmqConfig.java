package com.hongcha.hcmq.clent.config;

public class HcmqConfig {

    private String brokerHost = "127.0.0.1";

    private int brokerPort = 9999;

    private String username = "hcmq";

    private String password = "hcmq";

    private String groupName;

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

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public void setBrokerHost(String brokerHost) {
        this.brokerHost = brokerHost;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public void setBrokerPort(int brokerPort) {
        this.brokerPort = brokerPort;
    }
}
