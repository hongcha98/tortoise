package com.hongcha.hcmq.broker;

import com.hongcha.hcmq.broker.config.HcmqConfig;

public class HcmqMain {
    public static void main(String[] args) {
        HcmqConfig hcmqConfig = parse(args);
        Broker broker = new Broker(hcmqConfig);
        broker.start();
    }

    private static HcmqConfig parse(String[] args) {
        return new HcmqConfig();
    }
}
