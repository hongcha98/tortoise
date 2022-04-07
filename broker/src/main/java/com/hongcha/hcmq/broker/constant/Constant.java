package com.hongcha.hcmq.broker.constant;

public class Constant {

    public static final int QUEUE_FILE_ADD_SIZE = 1024 * 1024 * 10;

    public static final double QUEUE_FILE_SIZE_EXPANSION_PERCENTAGE = 0.75;

    public static final int PROTOCOL_CODE = 2;

    public static final String DELIMITER = "-";

    public static final String FILE_NAME_SUFFIX = ".hc";

    public static final String OFFSET_FILE_NAME = "offset" + FILE_NAME_SUFFIX;

    public static final int OFFSET_INIT = 4;


}
