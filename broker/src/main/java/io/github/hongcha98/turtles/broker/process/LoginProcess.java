package io.github.hongcha98.turtles.broker.process;

import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.config.TurtlesConfig;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.common.dto.login.LoginMessageReq;
import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

public class LoginProcess extends AbstractProcess {
    public LoginProcess(TurtlesBroker turtlesBroker) {
        super(turtlesBroker);
    }

    @Override
    public void process(ChannelHandlerContext channelHandlerContext, Message message) {
        doProcess(channelHandlerContext, message);
    }

    @Override
    public void doProcess(ChannelHandlerContext channelHandlerContext, Message message) {
        LoginMessageReq loginMessageReq = ProtocolUtils.decode(message, LoginMessageReq.class);
        TurtlesConfig turtlesConfig = getBroker().getTurtlesConfig();
        boolean flag = Objects.equals(turtlesConfig.getUsername(), loginMessageReq.getUsername()) && Objects.equals(turtlesConfig.getPassword(), loginMessageReq.getPassword());
        response(channelHandlerContext, message, flag);
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setLoginFlag(flag);
    }

}
