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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
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

    private Lock readLock;

    private Lock writeLock;

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
            ReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
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
     * ???????????? ????????????
     *
     * @param offset ?????????
     * @return
     */
    public MessageEntry getMessage(int offset) {
        return getMessage(offset, false);
    }

    /**
     * ??????offset???message,??????offset??????????????????null
     *
     * @param offset   ?????????
     * @param consumer ????????????
     * @return
     */
    public MessageEntry getMessage(int offset, boolean consumer) {
        boolean isLock = false;
        try {
            isLock = readLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            ByteBuffer buffer = mappedByteBuffer.duplicate();
            // ??????????????????
            int length = buffer.getInt(offset);
            if (length == 0) return null;
            int currentPosition = offset;
            buffer.position(currentPosition += Constant.MESSAGE_LENGTH);
            byte[] idBytes = new byte[36];
            buffer.get(idBytes);
            // id
            String id = new String(idBytes, StandardCharsets.UTF_8);
            // ????????????
            long createTime = buffer.getLong(currentPosition += Constant.MESSAGE_ID_LENGTH);
            // ??????????????????
            byte consumptionTimes = buffer.get(currentPosition += Constant.MESSAGE_CREATE_TIME_LENGTH);
            if (consumer) {
                consumptionTimes += 1;
                buffer.put(currentPosition, consumptionTimes);
            }
            // ??????header
            int headerLength = buffer.getInt(currentPosition += Constant.CONSUMER_NUMBER_LENGTH);
            buffer.position(currentPosition += Constant.MESSAGE_HEADER_LENGTH);
            byte[] headers = new byte[headerLength];
            buffer.get(headers);
            Map<String, String> header = JSON.parseObject(headers, Map.class);
            // ??????body
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

    /**
     * ?????????????????????,?????????????????????offset,?????????
     *
     * @param messageEntry ??????
     * @return
     */
    public int addMessage(MessageEntry messageEntry) {
        return addMessage(messageEntry, false);
    }

    /**
     * ?????????????????????,?????????????????????offset
     *
     * @param messageEntry ??????
     * @param brush        ????????????
     * @return
     */
    public int addMessage(MessageEntry messageEntry, boolean brush) {
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
            if (brush) {
                mappedByteBuffer.force();
            }
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
        // ????????????
        int messageLength = Constant.MESSAGE_METADATA_LENGTH + header.length + body.length;
        ByteBuffer buffer = ByteBuffer.allocate(Constant.MESSAGE_LENGTH + messageLength);
        buffer.putInt(messageLength);
        buffer.put(messageEntry.getId().getBytes(StandardCharsets.UTF_8));
        buffer.putLong(messageEntry.getCreateTime());
        buffer.put((byte) messageEntry.getConsumptionTimes());
        buffer.putInt(header.length);
        buffer.put(header);
        buffer.putInt(body.length);
        buffer.put(body);
        buffer.position(0);
        return buffer;
    }

    /**
     * ???????????????????????????
     *
     * @param time         ??????
     * @param offsetBefore ???????????????
     * @return
     */
    public int removeTimeBefore(long time, int offsetBefore) {
        boolean isLock = false;
        try {
            isLock = writeLock.tryLock(Constant.QUEUE_FILE_TRY_LOCK_TIME, TimeUnit.MILLISECONDS);
            int endOffset = Constant.FILE_LENGTH;
            int messageLength;
            // ?????????offset??????????????????
            while (endOffset < offsetBefore && (messageLength = mappedByteBuffer.getInt(endOffset)) != 0) {
                // ????????????
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
            int newStart = endOffset - Constant.FILE_LENGTH;
            //  ????????????
            long capacity = fileChannel.size();
            //  ????????????
            int newCapacity = (((newLength + Constant.FILE_LENGTH) / Constant.QUEUE_FILE_ADD_SIZE) + 1) * Constant.QUEUE_FILE_ADD_SIZE;
            int newPosition = Constant.FILE_LENGTH + newLength;
            mappedByteBuffer.putInt(newStart, newLength);
            mappedByteBuffer.force();
            MMAP_CLEAR_METHOD.invoke(FileChannelImpl.class, mappedByteBuffer);
            mappedByteBuffer = null;
            // ?????????????????????????????????
            fileChannel.position(0);
            fileChannel.transferTo(newStart, newPosition, fileChannel);
            // ??????
            fileChannel.truncate(newPosition);
            fileChannel.force(true);
            fileChannel.position(0);
            // ??????????????????
            if (capacity != newCapacity) {
                randomAccessFile.setLength(newCapacity);
            }
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, newCapacity);
            mappedByteBuffer.position(newPosition);
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
     * ??????
     */
    public void brush() {
        if (mappedByteBuffer != null) {
            mappedByteBuffer.force();
        }
    }

    /**
     * ????????????
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
     * ?????????????????????,??????????????????????????????offset
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
