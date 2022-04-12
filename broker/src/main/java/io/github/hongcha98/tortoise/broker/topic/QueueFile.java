package io.github.hongcha98.tortoise.broker.topic;

import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.tortoise.broker.LifeCycle;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.common.dto.message.Message;
import io.github.hongcha98.tortoise.common.dto.message.MessageInfo;
import io.github.hongcha98.tortoise.common.error.TortoiseException;
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

    private Protocol protocol;

    private ReentrantReadWriteLock.ReadLock readLock;

    private ReentrantReadWriteLock.WriteLock writeLock;

    public QueueFile(File file, Integer id, Protocol protocol) {
        try {
            this.id = id;
            long fileLength = Constant.QUEUE_FILE_ADD_SIZE;
            if (file.exists()) {
                fileLength = file.length();
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength);
            this.protocol = protocol;
            ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
            this.readLock = reentrantReadWriteLock.readLock();
            this.writeLock = reentrantReadWriteLock.writeLock();
            initOffset();
        } catch (Exception e) {
            throw new TortoiseException("queue error", e);
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
            //获取旧的position，后面设置回去
            int oldPosition = mappedByteBuffer.position();
            try {
                mappedByteBuffer.position(offset);
                // 读取消息长度
                int length = mappedByteBuffer.getInt(offset);
                if (length == 0) return null;
                // 创建时间
                long createTime = mappedByteBuffer.getLong(offset + Constant.MESSAGE_LENGTH);
                // 读取消费次数
                int consumptionTimesOffset = offset + Constant.MESSAGE_LENGTH + Constant.MESSAGE_CREATE_TIME_LENGTH;
                byte consumptionTimes = mappedByteBuffer.get(consumptionTimesOffset);
                if (consumer) {
                    consumptionTimes += 1;
                    mappedByteBuffer.put(consumptionTimesOffset, consumptionTimes);
                }
                mappedByteBuffer.position(offset + Constant.MESSAGE_METADATA_LENGTH);
                byte[] bytes = new byte[length];
                mappedByteBuffer.get(bytes);
                Message message = protocol.decode(bytes, Message.class);
                int nextOffset = offset + Constant.MESSAGE_METADATA_LENGTH + length;
                return new MessageInfo(message, createTime, consumptionTimes, offset, nextOffset);
            } finally {
                mappedByteBuffer.position(oldPosition);
            }
        } catch (InterruptedException e) {
            throw new TortoiseException(e);
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
            int offset = mappedByteBuffer.position();
            if ((double) offset / (double) mappedByteBuffer.capacity() >= Constant.QUEUE_FILE_SIZE_EXPANSION_PERCENTAGE) {
                mappedByteBuffer.force();
                MMAP_CLEAR_METHOD.invoke(FileChannelImpl.class, mappedByteBuffer);
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedByteBuffer.capacity() + Constant.QUEUE_FILE_ADD_SIZE);
                mappedByteBuffer.position(offset);
            }
            byte[] encode = protocol.encode(message);
            mappedByteBuffer.putInt(encode.length);
            mappedByteBuffer.putLong(System.currentTimeMillis());
            mappedByteBuffer.put((byte) 0);
            mappedByteBuffer.put(encode);
            mappedByteBuffer.putInt(Constant.FILE_LENGTH_INDEX, mappedByteBuffer.position());
            return offset;
        } catch (Exception e) {
            throw new TortoiseException(e);
        } finally {
            if (isLock) {
                writeLock.unlock();
            }
        }
    }

    /**
     * 删除时间之前的数据
     * @param time 时间
     * @return
     */
    public int removeTimeBefore(long time) {
        boolean isLock = false;
        try {
            isLock = writeLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            int endOffset = Constant.FILE_LENGTH;
            int messageLength;
            // 判断该offset是否含有数据
            while ((messageLength = mappedByteBuffer.getInt(endOffset)) != 0) {
                // 创建时间
                long createTime = mappedByteBuffer.getLong(endOffset + Constant.MESSAGE_LENGTH);
                if (createTime >= time) {
                    break;
                }
                endOffset = endOffset + Constant.MESSAGE_METADATA_LENGTH + messageLength;
            }
            int remove = endOffset - Constant.FILE_LENGTH;
            if (remove == 0) {
                return 0;
            }
            int newLength = mappedByteBuffer.position() - endOffset;
            byte[] bytes = new byte[newLength];
            mappedByteBuffer.position(endOffset);
            mappedByteBuffer.get(bytes);
            mappedByteBuffer.position(Constant.FILE_LENGTH);
            mappedByteBuffer.put(bytes);
            bytes = new byte[remove];
            mappedByteBuffer.put(bytes);
            mappedByteBuffer.putInt(Constant.FILE_LENGTH_INDEX, newLength);
            mappedByteBuffer.position(Constant.FILE_LENGTH + newLength);
            return remove;
        } catch (Exception e) {
            throw new TortoiseException(e);
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
