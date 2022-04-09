package io.github.hongcha98.turtles.broker.topic.queue.message;

import io.github.hongcha98.turtles.broker.topic.queue.Coding;
import io.github.hongcha98.turtles.broker.topic.queue.DefaultCoding;
import io.github.hongcha98.turtles.common.dto.message.Message;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class CodingTest {
    Coding coding;

    @Before
    public void init() {
        coding = new DefaultCoding();
    }

    @Test
    public void coding() {
        Message message = new Message();
        message.setId(UUID.randomUUID().toString());
        message.setBody("hello world".getBytes(StandardCharsets.UTF_8));
        byte[] encode = coding.encode(message);
        Message decode = coding.decode(ByteBuffer.wrap(encode), 0).getMessage();
        Assert.assertEquals(message, decode);
    }
}
