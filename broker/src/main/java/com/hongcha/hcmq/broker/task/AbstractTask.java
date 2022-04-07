package com.hongcha.hcmq.broker.task;

import com.hongcha.hcmq.broker.Broker;

public abstract class AbstractTask implements Runnable {
    private final Broker broker;

    protected AbstractTask(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        do {
            doRun();
        } while (runForever());
    }

    protected abstract void doRun();

    protected boolean runForever() {
        return true;
    }

    public Broker getBroker() {
        return broker;
    }
}
