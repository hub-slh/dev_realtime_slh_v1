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
    // Kafka配置
    private static final String kafka_botstrap_servers = "cdh01:9092"; // Kafka服务器地址
    private static final String kafka_cdc_db_topic = "topic_db_v1";    // 数据库变更数据主题
    private static final String kafka_user_info_topic = "topic_log";   // 用户行为日志主题

    // 从数据库加载的商品分类信息（静态变量，类加载时初始化）
    private static final List<DimBaseCategory> dim_base_categories;

    // 数据库连接对象
    private static Connection connection;

    // 评分模型权重系数
    private static final double device_rate_weight_coefficient = 0.1;  // 设备评分权重
    private static final double search_rate_weight_coefficient = 0.15; // 搜索评分权重

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
            throw new RuntimeException("初始化分类数据失败", e);
        } finally {
            // 关闭数据库连接
            if (connection != null){
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 获取分类数据的公共方法
    public static List<DimBaseCategory> getDimBaseCategories() {
        return dim_base_categories;
    }


    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户名（用于可能需要的HDFS操作）
        System.setProperty("HADOOP_USER_NAME", "root");

        // 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 1. 从Kafka消费CDC数据库变更数据
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(  // 构建安全的Kafka消费者
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),    // 消费者组ID使用当前时间
                        OffsetsInitializer.earliest() // 从最早偏移量开始消费
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
                "kafka_cdc_db_source"  // 算子名称
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");  // 设置算子UID和名称

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
                .process(new AggregateUserDataProcessFunction())  // 自定义聚合函数
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2))) // 2分钟滚动窗口
                .reduce((value1, value2) -> value2)  // 保留最新值
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");

        // 8. 应用设备和搜索评分模型
        win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories,device_rate_weight_coefficient,search_rate_weight_coefficient))
                .print();  // 打印结果

        // 9. 处理用户基本信息数据
        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

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

        // 11. 过滤出用户补充信息数据
        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter kafka user info sup")
                .name("filter kafka user info sup");

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
//        mapUserInfoSupDs.print();
        // 14. 过滤有效用户ID的数据
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());

        // 15. 按用户ID分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        // 16. 用户基本信息和补充信息的区间关联（5分钟时间窗口）
//        {"birthday":"1997-09-06","decade":1990,"uname":"南宫纨","gender":"F","zodiac_sign":"处女座","weight":"53",
//        "uid":"167","login_name":"4tjk9p8","unit_height":"cm","user_level":"1","phone_num":"13913669538","unit_weight":"kg",
//        "email":"hij36hcc@3721.net","ts_ms":1747052360467,"age":27,"height":"167"}
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))  // 前后5分钟的时间窗口
                .process(new IntervalJoinUserInfoLabelProcessFunc())  // 自定义关联处理函数
                .uid("process intervalJoin order info")
                .name("process intervalJoin order info");



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
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
    private static class AgePreferenceCoefficient {
        private static final Map<String, Map<String, Double>> agePreferenceMap = new HashMap<>();

        static {
            // 初始化潮流服饰的年龄偏好系数
            Map<String, Double> fashionMap = new HashMap<>();
            fashionMap.put("18-24", 0.9);
            fashionMap.put("25-29", 0.8);
            fashionMap.put("30-34", 0.6);
            fashionMap.put("35-39", 0.4);
            fashionMap.put("40-49", 0.2);
            fashionMap.put("50+", 0.1);
            agePreferenceMap.put("潮流服饰", fashionMap);

            // 初始化家居用品的年龄偏好系数
            Map<String, Double> homeMap = new HashMap<>();
            homeMap.put("18-24", 0.2);
            homeMap.put("25-29", 0.4);
            homeMap.put("30-34", 0.6);
            homeMap.put("35-39", 0.8);
            homeMap.put("40-49", 0.9);
            homeMap.put("50+", 0.7);
            agePreferenceMap.put("家居用品", homeMap);

            // 初始化健康食品的年龄偏好系数
            Map<String, Double> healthMap = new HashMap<>();
            healthMap.put("18-24", 0.1);
            healthMap.put("25-29", 0.2);
            healthMap.put("30-34", 0.4);
            healthMap.put("35-39", 0.6);
            healthMap.put("40-49", 0.8);
            healthMap.put("50+", 0.9);
            agePreferenceMap.put("健康食品", healthMap);
        }

        public static double getPreference(String category, String ageGroup) {
            return agePreferenceMap.getOrDefault(category, new HashMap<>())
                    .getOrDefault(ageGroup, 0.0);
        }

        public static String getAgeGroup(int age) {
            if (age >= 18 && age <= 24) return "18-24";
            else if (age >= 25 && age <= 29) return "25-29";
            else if (age >= 30 && age <= 34) return "30-34";
            else if (age >= 35 && age <= 39) return "35-39";
            else if (age >= 40 && age <= 49) return "40-49";
            else if (age >= 50) return "50+";
            else return "unknown";
        }
    }
}