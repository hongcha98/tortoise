package com.hongcha.hcmq.broker.process;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.config.HcmqConfig;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.hcmq.common.dto.login.LoginMessageReq;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class LoginProcess extends AbstractProcess {
    public LoginProcess(Broker broker) {
        super(broker);
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Message message) {
        doProcess(channelHandlerContext, message);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        LoginMessageReq loginMessageReq = ProtocolUtils.decode(message, LoginMessageReq.class);
        HcmqConfig hcmqConfig = getBroker().getHcmqConfig();
        boolean flag = Objects.equals(hcmqConfig.getUsername(), loginMessageReq.getUsername()) && Objects.equals(hcmqConfig.getPassword(), loginMessageReq.getPassword());
        response(channelHandlerContext, message, flag);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setLoginFlag(flag);
    }

}
