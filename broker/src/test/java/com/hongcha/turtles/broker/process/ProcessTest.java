package com.hongcha.turtles.broker.process;

import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.RemoteClient;
import com.hongcha.turtles.broker.TurtlesBroker;
import com.hongcha.turtles.broker.constant.ConstantTest;
import com.hongcha.turtles.common.dto.login.LoginMessageReq;
import com.hongcha.turtles.common.dto.message.MessageGetReq;
import com.hongcha.turtles.common.dto.message.MessageInfo;
import com.hongcha.turtles.common.dto.offset.OffsetGetReq;
import com.hongcha.turtles.common.dto.topic.GetSubscriptionMessageReq;
import com.hongcha.turtles.common.dto.topic.GetSubscriptionMessageResp;
import com.hongcha.turtles.common.dto.topic.SubscriptionMessageReq;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.hongcha.turtles.common.dto.constant.ProcessConstant.*;

public class ProcessTest {

    TurtlesBroker turtlesBroker;

    RemoteClient remoteClient;

    @Before
    public void init() throws Exception {
        turtlesBroker = new TurtlesBroker(ConstantTest.TURTLES_CONFIG);
        turtlesBroker.start();
        remoteClient = ConstantTest.REMOTE_CLIENT;
        remoteClient.start();
        // 先进行登录
        loginProcess();
    }

    @After
    public void close() throws Exception {
        remoteClient.close();
        turtlesBroker.close();
    }

    @Test
    public void loginProcess() throws ExecutionException, InterruptedException, TimeoutException {
        LoginMessageReq loginMessageReq = new LoginMessageReq();
        loginMessageReq.setUsername(ConstantTest.TURTLES_CONFIG.getUsername());
        loginMessageReq.setPassword(ConstantTest.TURTLES_CONFIG.getPassword());
        Message message = remoteClient.buildRequest(loginMessageReq, PROCESS_LOGIN);
        Boolean login = remoteClient.send("127.0.0.1", ConstantTest.TURTLES_CONFIG.getPort(), message, Boolean.class);
        Assert.assertTrue(login);
//        loginMessageReq.setPassword("error password");
//        message = remoteClient.buildRequest(loginMessageReq, PROCESS_LOGIN);
//        login = remoteClient.send("127.0.0.1", ConstantTest.TURTLES_CONFIG.getPort(), message, Boolean.class);
//        Assert.assertFalse(login);
    }

    @Test
    public void subscriptionProcess() throws ExecutionException, InterruptedException, TimeoutException {
        SubscriptionMessageReq subscriptionMessageReq = new SubscriptionMessageReq();
        subscriptionMessageReq.setGroupName(ConstantTest.GROUP);
        Set<String> topicNames = new HashSet<>();
        topicNames.add(ConstantTest.TOPIC_NAME);
        subscriptionMessageReq.setTopicNames(topicNames);
        Message message = remoteClient.buildRequest(subscriptionMessageReq, PROCESS_SUBSCRIPTION);
        // 没有登录会异常
        Boolean flag = remoteClient.send("127.0.0.1", ConstantTest.TURTLES_CONFIG.getPort(), message, Boolean.class);
        Assert.assertTrue(flag);
    }


    @Test
    public void getSubscriptionProcess() throws ExecutionException, InterruptedException, TimeoutException {
        subscriptionProcess();
        for (; ; ) {
            GetSubscriptionMessageReq getSubscriptionMessageReq = new GetSubscriptionMessageReq();
            Message message = remoteClient.buildRequest(getSubscriptionMessageReq, PROCESS_GET_SUBSCRIPTION);
            GetSubscriptionMessageResp resp = remoteClient.send("127.0.0.1", ConstantTest.TURTLES_CONFIG.getPort(), message, GetSubscriptionMessageResp.class);
            System.out.println("resp = " + resp);
            for (Set<Integer> queuesId : resp.getTopicQueuesIdMap().values()) {
                if (!queuesId.isEmpty()) {
                    return;
                }
            }
        }
    }

    @Test
    public void getOffsetProcess() throws ExecutionException, InterruptedException, TimeoutException {
        subscriptionProcess();
        OffsetGetReq offsetGetReq = new OffsetGetReq();
        offsetGetReq.setTopicName(ConstantTest.TOPIC_NAME);
        offsetGetReq.setQueueId(0);
        Message message = remoteClient.buildRequest(offsetGetReq, PROCESS_GET_OFFSET);
        int offset = remoteClient.send("127.0.0.1", ConstantTest.TURTLES_CONFIG.getPort(), message, Integer.class);
        System.out.println(offset);
    }


}
