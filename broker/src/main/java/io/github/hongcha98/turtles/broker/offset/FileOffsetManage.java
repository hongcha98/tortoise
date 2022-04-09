package io.github.hongcha98.turtles.broker.offset;

import io.github.hongcha98.remote.common.spi.SpiLoader;
import io.github.hongcha98.remote.protocol.Protocol;
import io.github.hongcha98.turtles.broker.constant.Constant;
import io.github.hongcha98.turtles.common.error.TurtlesException;
import io.github.hongcha98.turtles.broker.topic.TopicManage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class FileOffsetManage extends AbstractOffsetManage {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetManage.class);

    private File file;

    private RandomAccessFile randomAccessFile;

    public FileOffsetManage(File file, TopicManage topicManage) {
        super(topicManage);
        this.file = file;
    }


    @Override
    protected void doClose() {
        enduranceCommon();
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            log.error("file close error", e);
        }
    }

    @Override
    protected void initAllOffset() {
        try {
            boolean exists = file.exists();
            if (!exists) {
                file.createNewFile();
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            if (exists) {
                long length = file.length();
                if (length != 0) {
                    byte[] bytes = new byte[(int) length];
                    randomAccessFile.read(bytes);
                    setTopicGroupOffsetMap(SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE).decode(bytes, Map.class));
                }
            }
        } catch (Exception e) {
            throw new TurtlesException("offset manage error", e);
        }
    }

    private void enduranceCommon() {
        synchronized (this) {
            byte[] encode = SpiLoader.load(Protocol.class, Constant.PROTOCOL_CODE).encode(getTopicGroupOffsetMap());
            try {
                randomAccessFile.seek(0);
                randomAccessFile.setLength(encode.length);
                randomAccessFile.write(encode);
            } catch (IOException e) {
                log.info("write error", e);
            }
        }
    }
}
