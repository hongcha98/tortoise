package com.hongcha.hcmq.clent.consumer;

import com.hongcha.hcmq.common.dto.message.Message;


public interface MessageListener {
    void listener(Message message);
}
