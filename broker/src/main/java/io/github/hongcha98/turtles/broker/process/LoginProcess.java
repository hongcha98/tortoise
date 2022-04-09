package io.github.hongcha98.turtles.broker.process;

import io.github.hongcha98.remote.common.Message;
import io.github.hongcha98.remote.core.util.ProtocolUtils;
import io.github.hongcha98.turtles.broker.TurtlesBroker;
import io.github.hongcha98.turtles.broker.config.TurtlesConfig;
import io.github.hongcha98.turtles.broker.context.ChannelContext;
import io.github.hongcha98.turtles.common.dto.login.LoginRequest;
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
        LoginRequest loginRequest = ProtocolUtils.decode(message, LoginRequest.class);
        TurtlesConfig turtlesConfig = getBroker().getTurtlesConfig();
        boolean flag = Objects.equals(turtlesConfig.getUsername(), loginRequest.getUsername()) && Objects.equals(turtlesConfig.getPassword(), loginRequest.getPassword());
        ChannelContext channelContext = getBroker().getChannelContextManage().getChannelContext(channelHandlerContext.channel());
        channelContext.setLoginFlag(flag);
        response(channelHandlerContext, message, flag);
    }

}
