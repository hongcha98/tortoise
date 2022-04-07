package com.hongcha.hcmq.broker.task;

import com.hongcha.hcmq.broker.LifeCycle;

public interface TaskManage extends LifeCycle {

    void addTask(AbstractTask task);

}
