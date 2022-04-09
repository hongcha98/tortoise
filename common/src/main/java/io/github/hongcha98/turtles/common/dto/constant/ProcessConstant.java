package io.github.hongcha98.turtles.common.dto.constant;

public class ProcessConstant {
    /**
     * session相关处理器code  1 - 100
     */
    public static final int PROCESS_LOGIN = 0; // 登录

    public static final int PROCESS_SUBSCRIPTION = 1; // 订阅

    public static final int PROCESS_UNSUBSCRIPTION = 2; //取消订阅

    public static final int PROCESS_SUBSCRIPTION_INFO = 3; // 当前通道订阅信息

    /**
     * offset相关处理器code 101-200
     */
    public static final int PROCESS_OFFSET_GET = 101; // 获取offset

    public static final int PROCESS_OFFSET_COMMIT = 102; // 提交offset

    /**
     * topic相关处理器code 201-300
     */
    public static final int PROCESS_TOPIC_CREATE = 201; // 创建topic

    public static final int PROCESS_TOPIC_DELETE = 202; // 删除topic

    /**
     * message相关处理器code 301-400
     */
    public static final int PROCESS_MESSAGE_SESSION_PULL = 301;  // 当前通道拉取消息

    public static final int PROCESS_MESSAGE_ADD = 302;  // 添加消息


}
