package com.hongcha.turtles.broker.offset;

import com.hongcha.turtles.broker.LifeCycle;

import java.util.Map;

public interface OffsetManage extends LifeCycle {

    Map<Integer, Integer> getOffset(String topic, String group);

    int getOffset(String topic, String group, int id);

    void commitOffset(String topic, String group, int id, int offset);

}
