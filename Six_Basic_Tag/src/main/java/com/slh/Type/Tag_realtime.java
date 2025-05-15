package com.slh.Type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.function.*;
import com.slh.utils.JdbcUtils;
import com.slh.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
/**
 * 实时用户标签处理应用，主要功能：
 * 1. 从Kafka消费用户数据和行为日志
 * 2. 对数据进行处理和丰富
 * 3. 计算用户年龄、星座等属性
 * 4. 关联用户基本信息和补充信息
 * 5. 应用设备和搜索评分模型
 * @author song.lihao
 * @date 2025/5/13
 */
public class Tag_realtime {
    // Kafka服务器地址
    private static final String kafka_botstrap_servers = "cdh01:9092";
    // 数据库变更数据主题
    private static final String kafka_cdc_db_topic = "topic_db_v1";
    // 用户行为日志主题
    private static final String kafka_user_info_topic = "topic_log";
    private static final String kafka_label_base2_topic = "kafka_result_label_base2";
    private static final String kafka_label_base4_topic = "kafka_result_label_base4";
    private static final String kafka_label_base6_topic = "kafka_result_label_base6";
    // 从数据库加载的商品分类信息（静态变量，类加载时初始化）
    private static final List<DimBaseCategory> dim_base_categories;
    // 数据库连接对象
    private static Connection connection;
    // 设备评分权重
    public class LabelConfig {
        private static final double device_rate_weight_coefficient = 0.1;
        // 搜索评分权重
        private static final double search_rate_weight_coefficient = 0.15;
        // 时间权重系数
        private static final double time_rate_weight_coefficient = 0.1;
        // 价格权重系数
        private static final double amount_rate_weight_coefficient = 0.15;
        // 品牌权重系数
        private static final double brand_rate_weight_coefficient = 0.2;
        // 类目权重系数
        private static final double category_rate_weight_coefficient = 0.3;
    }
    // 静态初始化块 - 在类加载时执行，从数据库加载商品分类数据
    static {
        try {
            // 建立数据库连接
            connection = DriverManager.getConnection("jdbc:mysql://cdh03:3306/dev_realtime_v1", "root", "root");
            // SQL查询：获取三级商品分类及其对应的二级和一级分类名称
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from dev_realtime_v1.base_category3 as b3  \n" +
                    "     join dev_realtime_v1.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join dev_realtime_v1.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            // 使用JDBC工具类执行查询并映射为DimBaseCategory对象列表
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            // 若初始化分类数据失败，抛出运行时异常
            throw new RuntimeException("初始化分类数据失败", e);
        } finally {
            // 关闭数据库连接
            if (connection != null){
                try {
                    connection.close();
                } catch (Exception e) {
                    System.err.println("关闭数据库连接失败：" + e.getMessage());
                }
            }
        }
    }
    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户名（用于可能需要的HDFS操作）
        System.setProperty("HADOOP_USER_NAME", "root");
        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4
        env.setParallelism(4);

        // 1. 从Kafka消费CDC数据库变更数据
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                // 构建安全的Kafka消费者
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        // 消费者组ID使用当前时间
                        new Date().toString(),
                        // 从最早偏移量开始消费
                        OffsetsInitializer.earliest()
                ),
                // 定义水印策略（允许3秒的乱序）
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    // 从消息中提取时间戳（ts_ms字段）
                                    JSONObject jsonObject = JSONObject.parseObject(event);
                                    if (event != null && jsonObject.containsKey("ts_ms")){
                                        try {
                                            return jsonObject.getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("解析JSON或获取ts_ms失败: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                // 算子名称
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");  // 设置算子UID和名称
//        kafkaCdcDbSource.print();
        // 2. 从Kafka消费用户页面日志数据（结构与CDC数据类似）
        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_user_info_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return jsonObject.getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("解析JSON或获取ts_ms失败: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_page_log_source"
                ).uid("kafka_page_log_source")
                .name("kafka_page_log_source");
//        kafkaPageLogSource.print();
        // 3. 数据转换：将Kafka消息字符串转为JSONObject
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert json cdc db")
                .name("convert json cdc db");

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");

        // 4. 处理页面日志：提取设备信息和搜索关键词
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("get device info & search")
                .name("get device info & search");

        // 5. 过滤出有用户ID的日志记录并按用户ID分组
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        // 6. 处理重复时间戳的数据
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());

        // 7. 2分钟滚动窗口处理：按用户分组后聚合数据
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                // 自定义聚合函数
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                // 2分钟滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                // 保留最新值
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
//        win2MinutesPageLogsDs.print();
        // 8. 应用设备和搜索评分模型
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDS = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, LabelConfig.device_rate_weight_coefficient, LabelConfig.search_rate_weight_coefficient));


        // 9. 处理用户基本信息数据
        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter kafka order info")
                .name("filter kafka order info");

        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter kafka order detail")
                .name("filter kafka order detail");

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());

        // 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, LabelConfig.time_rate_weight_coefficient, LabelConfig.amount_rate_weight_coefficient, LabelConfig.brand_rate_weight_coefficient, LabelConfig.category_rate_weight_coefficient));
//        mapOrderInfoAndDetailModelDs.print();
        // 10. 转换用户信息：处理生日字段（从epochDay转为日期字符串）
        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = userInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });
//        finalUserInfoDs.print("a->");
        // 11. 过滤出用户补充信息数据
        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter kafka user info sup")
                .name("filter kafka user info sup");
//        userInfoSupDs.print();
        // 12. 转换用户基本信息：提取关键字段并计算年龄、星座等
        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            // 提取基本字段
                            result.put("uid", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));

                            // 计算年龄和年代
                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                try {
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    // 计算星座
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        return result;
                    }
                })
                .uid("map userInfo ds")
                .name("map userInfo ds");
//        mapUserInfoDs.print();

        // 13. 转换用户补充信息：提取关键字段
        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = userInfoSupDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                })
                .uid("sup userinfo sup")
                .name("sup userinfo sup");
//        mapUserInfoSupDs.print("a->");

        // 14. 过滤有效用户ID的数据
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
//        finalUserinfoDs.print("1->");
//        finalUserinfoSupDs.print("2->");
        // 15. 按用户ID分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));
//        keyedStreamUserInfoDs.print("1->");
//        keyedStreamUserInfoSupDs.print("2->");
        // 16. 用户基本信息和补充信息的区间关联（5分钟时间窗口）
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                // 前后5分钟的时间窗口
                .between(Time.minutes(-5), Time.minutes(5))
                // 自定义关联处理函数
                .process(new IntervalJoinUserInfoLabelProcessFunc())
                .uid("process intervalJoin order info")
                .name("process intervalJoin order info");
//        processIntervalJoinUserInfo6BaseMessageDs.print();
//        // 将设备和搜索评分结果写入Kafka
//        mapDeviceAndSearchRateResultDS.map(data->data.toJSONString())
//                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_label_base2_topic));
//        // 将订单信息和详情模型结果写入Kafka
//        mapOrderInfoAndDetailModelDs.map(data->data.toJSONString())
//                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_label_base4_topic));
//        // 将用户基本信息和补充信息关联结果写入Kafka
//        processIntervalJoinUserInfo6BaseMessageDs.map(data->data.toJSONString())
//                .sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_label_base6_topic));
        // 将数据写入到CSV文件
        processIntervalJoinUserInfo6BaseMessageDs.writeAsText("com/slh/output/output.csv").setParallelism(1);
        mapOrderInfoAndDetailModelDs.writeAsText("com/slh/output/output.csv").setParallelism(1);
        mapDeviceAndSearchRateResultDS.writeAsText("com/slh/output/output.csv").setParallelism(1);
        // 打印用户基本信息和补充信息关联结果
        processIntervalJoinUserInfo6BaseMessageDs.print();
        // 打印订单信息和详情模型结果
        mapOrderInfoAndDetailModelDs.print();
        // 打印设备和搜索评分结果
        mapDeviceAndSearchRateResultDS.print();

        // 执行Flink作业
        env.execute("DbusUserInfo6BaseLabel");
    }

    /**
     * 计算年龄
     * @param birthDate 出生日期
     * @param currentDate 当前日期
     * @return 年龄
     */
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    /**
     * 根据出生日期计算星座
     * @param birthDate 出生日期
     * @return 星座名称
     */
    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();
        // 星座日期范围判断
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) return "水瓶座";
        else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) return "双鱼座";
        else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) return "白羊座";
        else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) return "金牛座";
        else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) return "双子座";
        else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) return "巨蟹座";
        else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) return "狮子座";
        else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) return "处女座";
        else if ((month == 9 && day >= 23) || (month == 10 && day <= 22)) return "天秤座";
        else if ((month == 10 && day >= 23) || (month == 11 && day <= 22)) return "天蝎座";
        else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) return "射手座";
        else return "未知";
    }
}