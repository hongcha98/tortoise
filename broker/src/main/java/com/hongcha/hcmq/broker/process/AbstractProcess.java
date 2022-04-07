package com.hongcha.hcmq.broker.process;

import com.hongcha.hcmq.broker.Broker;
import com.hongcha.hcmq.broker.context.ChannelContext;
import com.hongcha.remote.common.Message;
import com.hongcha.remote.common.constant.RemoteConstant;
import com.hongcha.remote.common.exception.RemoteExceptionBody;
import com.hongcha.remote.common.process.Process;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProcess implements Process {
    public final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Broker broker;

    public AbstractProcess(Broker broker) {
        this.broker = broker;
    }

    public Broker getBroker() {
        return broker;
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Message message) {
        if (isLogin(channelHandlerContext)) {
            try {
                doProcess(channelHandlerContext, message);
            } catch (Exception e) {
                log.error("process error", e);
                responseException(channelHandlerContext, message, e);
            }
        } else {
            responseException(channelHandlerContext, message, new IllegalStateException("not login"));
        }
    }

    protected abstract void doProcess(ChannelHandlerContext channelHandlerContext, Message message) throws Exception;


    protected boolean isLogin(ChannelHandlerContext channelHandlerContext) {
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        return channelContext.isLoginFlag();
    }

    protected void response(ChannelHandlerContext channelHandlerContext, Message message, Object msg) {
        response(channelHandlerContext, message, msg, message.getCode());
    }

    protected void response(ChannelHandlerContext channelHandlerContext, Message message, Object msg, int code) {
        Message response = getBroker().getRemoteServer().buildResponse(message, msg, code);
        channelHandlerContext.writeAndFlush(response);
    }

    protected void responseException(ChannelHandlerContext channelHandlerContext, Message message, Exception exception) {
        RemoteExceptionBody remoteExceptionBody = new RemoteExceptionBody(exception);
        Message resp = getBroker().getRemoteServer().buildResponse(message, remoteExceptionBody, RemoteConstant.ERROR_CODE);
        channelHandlerContext.writeAndFlush(resp);
    }
}
