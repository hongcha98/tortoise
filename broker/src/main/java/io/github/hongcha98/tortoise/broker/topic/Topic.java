package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.broker.LifeCycle;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.common.dto.message.Message;
import io.github.hongcha98.tortoise.common.dto.message.MessageInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

public class Topic implements LifeCycle {
    AtomicInteger polling = new AtomicInteger(0);
    /**
     * 存储位置
     */
    private final String path;
    /**
     * 主题名字
     */
    private final String name;
    /**
     * 队列数量
     */
    private final int queueNumber;
    /**
     * 编码解码器
     */
    private final Protocol protocol;

    /**
     * 队列列表
     */
    Map<Integer/* id */, QueueFile> queueFileMap;

    public Topic(String path, String name, int queueNumber, Protocol protocol) {
        if (queueNumber < 1) {
            throw new IllegalStateException("queue number < 1");
        }
        this.path = path;
        this.name = name;
        this.queueNumber = queueNumber;
        this.protocol = protocol;
        this.queueFileMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        String topicPath = path + File.separator + name;
        File topicDirectory = new File(topicPath);
        if (!topicDirectory.exists()) {
            topicDirectory.mkdir();
        }
        for (int i = 0; i < queueNumber; i++) {
            String queueFileName = topicPath + File.separator + i + Constant.FILE_NAME_SUFFIX;
            File file = new File(queueFileName);
            QueueFile queueFile = new QueueFile(file, i, protocol);
            queueFileMap.put(queueFile.getId(), queueFile);
        }
    }

    public MessageInfo getMessage(int id, int offset) {
        return getMessage(id, offset, false);
    }

    public MessageInfo getMessage(int id, int offset, boolean consumer) {
        return queueFileMap.get(id).getMessage(offset, consumer);
    }

    public int addMessage(Message message) {
        return addMessage(message, false);
    }

    public int addMessage(Message message, boolean brush) {
        int position = polling.getAndIncrement();
        if (position == Integer.MAX_VALUE) {
            polling.set(0);
        }
        List<Integer> queueIds = new ArrayList<>(getQueuesId());
        int id = queueIds.get(position % queueIds.size());
        return addMessage(id, message, brush);
    }

    public int addMessage(int id, Message message) {
        return addMessage(id, message, false);
    }

    public int addMessage(int id, Message message, boolean brush) {
        QueueFile queueFile = queueFileMap.get(id);
        int offset = queueFile.addMessage(message);
        if (brush) {
            queueFile.brush();
        }
        return offset;
    }

    public int removeTimeBefore(int id, long time) {
        return queueFileMap.get(id).removeTimeBefore(time);
    }


    public int getIdOffset(int id) {
        return queueFileMap.get(id).getPosition();
    }


    public String getName() {
        return name;
    }

    public int getQueueNumber() {
        return queueNumber;
    }


    public Set<Integer> getQueuesId() {
        return queueFileMap.keySet();
    }


    @Override
    public void close() {
        for (QueueFile queueFile : queueFileMap.values()) {
            queueFile.close();
        }
    }

    public void brush() {
        for (QueueFile queueFile : queueFileMap.values()) {
            queueFile.brush();
        }
    }
}
