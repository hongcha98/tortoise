package com.hongcha.hcmq.broker.offset;

import com.hongcha.hcmq.broker.constant.ConstantTest;
import com.hongcha.hcmq.broker.topic.DefaultTopicManage;
import com.hongcha.hcmq.broker.topic.TopicManage;
import com.hongcha.hcmq.common.dto.message.MessageInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class OffsetManageTest {
    OffsetManage offsetManage;

    TopicManage topicManage;

    @Before
    public void init() {
        topicManage = new DefaultTopicManage(ConstantTest.HCMQ_CONFIG);
        offsetManage = new FileOffsetManage(new File(ConstantTest.OFFSET_FILE_NAME), topicManage);
        topicManage.start();
    }

    @After
    public void close() {
        offsetManage.close();
        topicManage.close();
    }

    @Test
    public void allTest() {
        String group = "consumer-group";
        int index = 0;
        for (int i = 0; ; i++) {
            int offset = offsetManage.getOffset(ConstantTest.TOPIC_NAME, group, index);
            System.out.println("第" + i + "条消息 offset :" + offset);
            MessageInfo message = topicManage.getTopic(ConstantTest.TOPIC_NAME).getMessage(index, offset);
            System.out.println(message);
            if (message == null) break;
            offsetManage.commitOffset(ConstantTest.TOPIC_NAME, group, index, message.getNextOffset());
        }
    }
}
