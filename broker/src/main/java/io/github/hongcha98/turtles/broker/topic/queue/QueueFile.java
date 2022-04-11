package io.github.hongcha98.turtles.broker.topic.queue;

import io.github.hongcha98.turtles.broker.LifeCycle;
import io.github.hongcha98.turtles.broker.constant.Constant;
import io.github.hongcha98.turtles.common.dto.message.Message;
import io.github.hongcha98.turtles.common.dto.message.MessageInfo;
import io.github.hongcha98.turtles.common.error.TurtlesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class QueueFile implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(QueueFile.class);

    private static final Method MMAP_CLEAR_METHOD;

    static {
        try {
            MMAP_CLEAR_METHOD = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            MMAP_CLEAR_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private Integer id;

    private RandomAccessFile randomAccessFile;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private Coding coding;

    private ReentrantReadWriteLock.ReadLock readLock;

    private ReentrantReadWriteLock.WriteLock writeLock;

    public QueueFile(File file, Integer id, Coding coding) {
        try {
            this.id = id;
            long fileLength = Constant.QUEUE_FILE_ADD_SIZE;
            if (file.exists()) {
                fileLength = file.length();
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength);
            this.coding = coding;
            ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
            this.readLock = reentrantReadWriteLock.readLock();
            this.writeLock = reentrantReadWriteLock.writeLock();
            initOffset();
        } catch (Exception e) {
            throw new TurtlesException("queue error", e);
        }
    }

    private void initOffset() {
        int offset = mappedByteBuffer.getInt(Constant.FILE_LENGTH_INDEX);
        if (offset == 0) {
            offset = Constant.FILE_LENGTH;
        }
        mappedByteBuffer.position(offset);
    }

    /**
     * 单纯获取 不是消费
     *
     * @param offset 偏移量
     * @return
     */
    public MessageInfo getMessage(int offset) {
        return getMessage(offset, false);
    }

    /**
     * 获取offset的message,如果offset处没有则返回null
     *
     * @param offset   偏移量
     * @param consumer 是否消费
     * @return
     */
    public MessageInfo getMessage(int offset, boolean consumer) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            return coding.decode(mappedByteBuffer, offset, consumer);
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
            isLock = writeLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            message.setId(UUID.randomUUID().toString());
            message.setCreateTime(System.currentTimeMillis());
            int offset = mappedByteBuffer.position();
            if ((double) offset / (double) mappedByteBuffer.capacity() >= Constant.QUEUE_FILE_SIZE_EXPANSION_PERCENTAGE) {
                mappedByteBuffer.force();
                MMAP_CLEAR_METHOD.invoke(FileChannelImpl.class, mappedByteBuffer);
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedByteBuffer.capacity() + Constant.QUEUE_FILE_ADD_SIZE);
                mappedByteBuffer.position(offset);
            }
            byte[] encode = coding.encode(message);
            mappedByteBuffer.put(encode);
            mappedByteBuffer.putInt(Constant.FILE_LENGTH_INDEX, mappedByteBuffer.position());
            return offset;
        } catch (Exception e) {
            throw new TurtlesException(e);
        } finally {
            if (isLock) {
                writeLock.unlock();
            }
        }
    }

    /**
     * 刷盘
     */
    public void brush() {
        if (mappedByteBuffer != null) {
            mappedByteBuffer.force();
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        writeLock.lock();
        try {
            mappedByteBuffer.force();
            MMAP_CLEAR_METHOD.invoke(FileChannelImpl.class, mappedByteBuffer);
            mappedByteBuffer = null;
            fileChannel.close();
            fileChannel = null;
            randomAccessFile.close();
            randomAccessFile = null;
        } catch (Exception e) {
            LOG.error("queue id :" + id + " close error", e);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 获取写入的位置
     *
     * @return
     */
    public int getPosition() {
        return mappedByteBuffer.position();
    }

    public Integer getId() {
        return id;
    }

}
