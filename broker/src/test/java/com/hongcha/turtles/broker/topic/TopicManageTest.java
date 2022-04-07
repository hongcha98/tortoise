package com.hongcha.turtles.broker.topic;

import com.hongcha.turtles.broker.constant.ConstantTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TopicManageTest {

    TopicManage topicManage;

    @Before
    public void init() {
        topicManage = new DefaultTopicManage(ConstantTest.TURTLES_CONFIG);
        topicManage.start();
    }

    @After
    public void close() {
        topicManage.close();
    }


    @Test
    public void addTopic() {
        topicManage.addTopic(ConstantTest.TOPIC_NAME, 4);
    }


    @Test
    public void getAllTopic() {
        Map<String, Topic> allTopic = topicManage.getAllTopic();
        System.out.println("allTopic = " + allTopic);
    }


    @Test
    public void deleteTopic() {
        topicManage.deleteTopic(ConstantTest.TOPIC_NAME);
    }


}
