package com.hongcha.turtles.broker.task;

import com.hongcha.turtles.broker.LifeCycle;

public interface TaskManage extends LifeCycle {

    void addTask(AbstractTask task);

}
