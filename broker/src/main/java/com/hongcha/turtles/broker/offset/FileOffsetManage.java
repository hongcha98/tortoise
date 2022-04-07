package com.hongcha.turtles.broker.offset;

import com.hongcha.turtles.broker.constant.Constant;
import com.hongcha.turtles.broker.error.TurtlesException;
import com.hongcha.turtles.broker.topic.TopicManage;
import com.hongcha.remote.common.spi.SpiLoader;
import com.hongcha.remote.protocol.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class FileOffsetManage extends AbstractOffsetManage {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetManage.class);

    private RandomAccessFile randomAccessFile;

    public FileOffsetManage(File file, TopicManage topicManage) {
        super(topicManage);
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


    @Override
    protected void endurance(String topic, String group, int id, int offset) {
        enduranceCommon();
    }

    @Override
    protected void enduranceAll() {
        enduranceCommon();
    }

    @Override
    protected void enduranceTopic(String topic) {
        enduranceCommon();
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
