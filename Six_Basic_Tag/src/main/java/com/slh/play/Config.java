package com.slh.play;

/**
 * @Package com.slh.play.Config
 * @Author song.lihao
 * @Date 2025/5/15 18:37
 * @description:
 */
public class Config {
    // Kafka配置
    public static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092";
    public static final String KAFKA_CDC_DB_TOPIC = "topic_db_v1";
    public static final String KAFKA_USER_INFO_TOPIC = "topic_log";
    public static final String KAFKA_LABEL_BASE2_TOPIC = "kafka_result_label_base2";
    public static final String KAFKA_LABEL_BASE4_TOPIC = "kafka_result_label_base4";
    public static final String KAFKA_LABEL_BASE6_TOPIC = "kafka_result_label_base6";

    // 数据库配置
    public static final String DB_URL = "jdbc:mysql://cdh03:3306/dev_realtime_v1";
    public static final String DB_USER = "root";
    public static final String DB_PASSWORD = "root";

    // 评分模型权重
    public static class LabelConfig {
        public static final double DEVICE_RATE_WEIGHT = 0.1;
        public static final double SEARCH_RATE_WEIGHT = 0.15;
        public static final double TIME_RATE_WEIGHT = 0.1;
        public static final double AMOUNT_RATE_WEIGHT = 0.15;
        public static final double BRAND_RATE_WEIGHT = 0.2;
        public static final double CATEGORY_RATE_WEIGHT = 0.3;
    }
}
