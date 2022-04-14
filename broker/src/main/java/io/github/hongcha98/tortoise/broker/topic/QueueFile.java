package io.github.hongcha98.tortoise.broker.topic;

import com.alibaba.fastjson.JSON;
import io.github.hongcha98.tortoise.broker.LifeCycle;
import io.github.hongcha98.tortoise.broker.constant.Constant;
import io.github.hongcha98.tortoise.common.dto.message.MessageEntry;
import io.github.hongcha98.tortoise.common.error.TortoiseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
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


    private ReentrantReadWriteLock.ReadLock readLock;

    private ReentrantReadWriteLock.WriteLock writeLock;

    public QueueFile(File file, Integer id) {
        try {
            this.id = id;
            long fileLength = Constant.QUEUE_FILE_ADD_SIZE;
            if (file.exists()) {
                fileLength = file.length();
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength);
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
    public MessageEntry getMessage(int offset) {
        return getMessage(offset, false);
    }

    /**
     * 获取offset的message,如果offset处没有则返回null
     *
     * @param offset   偏移量
     * @param consumer 是否消费
     * @return
     */
    public MessageEntry getMessage(int offset, boolean consumer) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            ByteBuffer buffer = mappedByteBuffer.duplicate();
            // 读取消息长度
            int length = buffer.getInt(offset);
            if (length == 0) return null;
            int currentPosition = offset;
            buffer.position(currentPosition += Constant.MESSAGE_LENGTH);
            byte[] idBytes = new byte[36];
            buffer.get(idBytes);
            // id
            String id = new String(idBytes, StandardCharsets.UTF_8);
            // 创建时间
            long createTime = buffer.getLong(currentPosition += Constant.MESSAGE_ID_LENGTH);
            // 读取消费次数
            byte consumptionTimes = buffer.get(currentPosition += Constant.MESSAGE_CREATE_TIME_LENGTH);
            if (consumer) {
                consumptionTimes += 1;
                buffer.put(currentPosition, consumptionTimes);
            }
            // 读取header
            int headerLength = buffer.getInt(currentPosition += Constant.CONSUMER_NUMBER_LENGTH);
            buffer.position(currentPosition += Constant.MESSAGE_HEADER_LENGTH);
            byte[] headers = new byte[headerLength];
            buffer.get(headers);
            Map<String, String> header = JSON.parseObject(headers, Map.class);
            // 读取body
            int bodyLength = buffer.getInt(currentPosition += headerLength);
            buffer.position(currentPosition += Constant.MESSAGE_BODY_LENGTH);
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            currentPosition += bodyLength;
            int nextOffset = currentPosition;
            MessageEntry messageEntry = new MessageEntry();
            messageEntry.setId(id);
            messageEntry.setCreateTime(createTime);
            messageEntry.setConsumptionTimes(consumptionTimes);
            messageEntry.setHeader(header);
            messageEntry.setBody(body);
            messageEntry.setOffset(offset);
            messageEntry.setNextOffset(nextOffset);
            return messageEntry;
        } catch (Exception e) {
            throw new TortoiseException(e);
        } finally {
            if (isLock) {
                readLock.unlock();
            }
        }
    }

    public String getMsgId(int offset) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            ByteBuffer readBuffer = mappedByteBuffer.asReadOnlyBuffer();
            // 读取消息长度
            int length = readBuffer.getInt(offset);
            if (length == 0) return null;
            readBuffer.position(offset += Constant.MESSAGE_LENGTH);
            byte[] idBytes = new byte[36];
            readBuffer.get(idBytes);
            return new String(idBytes, StandardCharsets.UTF_8);

        } catch (InterruptedException e) {
            throw new TortoiseException(e);
        } finally {
            if (isLock) {
                readLock.unlock();
            }
        }
    }


    public int getMsgIdOffset(String msgId, int begin, int max) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            ByteBuffer readBuffer = mappedByteBuffer.asReadOnlyBuffer();
            int offset = begin;
            for (int i = 0; i < max; i++) {
                // 读取消息长度
                int length = readBuffer.getInt(offset);
                if (length == 0) return -1;
                readBuffer.position(offset + Constant.MESSAGE_LENGTH);
                byte[] idBytes = new byte[36];
                readBuffer.get(idBytes);
                String id = new String(idBytes, StandardCharsets.UTF_8);
                if (id.equals(msgId))
                    return offset;
            }
            return -1;
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
     * @param messageEntry
     */
    public int addMessage(MessageEntry messageEntry) {
        ByteBuffer buffer = encodeMessage(messageEntry);
        boolean isLock = false;
        try {
            isLock = writeLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            int offset = mappedByteBuffer.position();
            if ((double) offset / (double) mappedByteBuffer.capacity() >= Constant.QUEUE_FILE_SIZE_EXPANSION_PERCENTAGE) {
                mappedByteBuffer.force();
                MMAP_CLEAR_METHOD.invoke(FileChannelImpl.class, mappedByteBuffer);
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedByteBuffer.capacity() + Constant.QUEUE_FILE_ADD_SIZE);
                mappedByteBuffer.position(offset);
            }
            mappedByteBuffer.put(buffer);
            mappedByteBuffer.putInt(Constant.FILE_LENGTH_INDEX, mappedByteBuffer.position());
            messageEntry.setOffset(offset);
            messageEntry.setNextOffset(mappedByteBuffer.position());
            return offset;
        } catch (Exception e) {
            throw new TortoiseException(e);
        } finally {
            if (isLock) {
                writeLock.unlock();
            }
        }
    }

    private ByteBuffer encodeMessage(MessageEntry messageEntry) {
        messageEntry.setId(UUID.randomUUID().toString());
        messageEntry.setCreateTime(System.currentTimeMillis());
        messageEntry.setConsumptionTimes(0);
        byte[] header = JSON.toJSONBytes(messageEntry.getHeader());
        byte[] body = messageEntry.getBody();
        // 消息长度
        int messageLength = Constant.MESSAGE_METADATA_LENGTH + header.length + body.length;
        ByteBuffer buffer = ByteBuffer.allocate(Constant.MESSAGE_LENGTH + messageLength);
        buffer.putInt(messageLength);
        buffer.put(messageEntry.getId().getBytes(StandardCharsets.UTF_8));
        buffer.putLong(messageEntry.getConsumptionTimes());
        buffer.put((byte) messageEntry.getConsumptionTimes());
        buffer.putInt(header.length);
        buffer.put(header);
        buffer.putInt(body.length);
        buffer.put(body);
        buffer.position(0);
        return buffer;
    }

    /**
     * 删除时间之前的数据
     *
     * @param time         时间
     * @param offsetBefore 偏移量之前
     * @return
     */
    public int removeTimeBefore(long time, int offsetBefore) {
        boolean isLock = false;
        try {
            isLock = writeLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            int endOffset = Constant.FILE_LENGTH;
            int messageLength;
            // 判断该offset是否含有数据
            while (endOffset <= offsetBefore && (messageLength = mappedByteBuffer.getInt(endOffset)) != 0) {
                // 创建时间
                long createTime = mappedByteBuffer.getLong(endOffset + Constant.MESSAGE_LENGTH + Constant.MESSAGE_ID_LENGTH);
                if (createTime >= time) {
                    break;
                }
                endOffset = endOffset + Constant.MESSAGE_LENGTH + messageLength;
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
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            return mappedByteBuffer.position();
        } catch (InterruptedException e) {
            throw new TortoiseException(e);
        } finally {
            if (isLock) {
                readLock.unlock();
            }
        }

    }

    public Integer getId() {
        return id;
    }

}
