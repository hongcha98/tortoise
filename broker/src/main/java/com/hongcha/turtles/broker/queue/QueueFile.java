package com.hongcha.turtles.broker.queue;

import com.hongcha.turtles.broker.LifeCycle;
import com.hongcha.turtles.broker.constant.Constant;
import com.hongcha.turtles.broker.error.TurtlesException;
import com.hongcha.turtles.common.dto.message.Message;
import com.hongcha.turtles.common.dto.message.MessageInfo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class QueueFile implements LifeCycle {
    private Integer id;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private Coding coding;

    private ReentrantReadWriteLock reentrantReadWriteLock;

    private ReentrantReadWriteLock.ReadLock readLock;

    private ReentrantReadWriteLock.WriteLock writeLock;

    public QueueFile(File file, Integer id, Coding coding) {
        try {
            this.id = id;
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Constant.QUEUE_FILE_ADD_SIZE);
            this.coding = coding;
            reentrantReadWriteLock = new ReentrantReadWriteLock();
            readLock = reentrantReadWriteLock.readLock();
            writeLock = reentrantReadWriteLock.writeLock();
            initOffset();
        } catch (Exception e) {
            throw new TurtlesException("queue error", e);
        }
    }

    private void initOffset() {
        int offset = this.mappedByteBuffer.getInt(0);
        if (offset == 0) {
            offset = Constant.OFFSET_INIT;
        }
        mappedByteBuffer.position(offset);
    }


    /**
     * 获取offset后面的message,如果offset处没有则返回null
     *
     * @param offset
     * @return
     */
    public MessageInfo getMessage(int offset) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(500, TimeUnit.MILLISECONDS);
            return coding.decode(mappedByteBuffer, offset);
        } catch (InterruptedException e) {
            throw new TurtlesException(e);
        } finally {
            if (isLock) {
                readLock.unlock();
            }
        }
    }

    /**
     * 添加数据到末尾,返回这条消息的offset
     *
     * @param message
     */
    public int addMessage(Message message) {
        boolean isLock = false;
        try {
            isLock = writeLock.tryLock(500, TimeUnit.MILLISECONDS);
            int offset = mappedByteBuffer.position();
            byte[] encode = coding.encode(message);
            mappedByteBuffer.put(encode);
            mappedByteBuffer.putInt(0, mappedByteBuffer.position());
            return offset;
        } catch (InterruptedException e) {
            throw new TurtlesException(e);
        } finally {
            if (isLock) {
                writeLock.unlock();
            }
        }
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {

        }
    }

    public int getPosition() {
        return mappedByteBuffer.position();
    }

    public Integer getId() {
        return id;
    }

}
