package com.slh.Type.Data_Transmission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.lzy.app.dwd.DwdApp
 * @Author zheyuan.liu
 * @Date 2025/5/12 21:46
 * @description:
 */

public class DwdDbApp {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 创建KafkaSource 消费名为 "topic_db_v1" 消费组为 "dwd_app"
        KafkaSource<String> kafkaSourceBd = FlinkSourceUtil.getKafkaSource("topic_db_v1", "dwd_app");

        DataStreamSource<String> kafka_source = env.fromSource(kafkaSourceBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 将 Kafka 消息解析为 JSONObject，并分配时间戳（延迟容忍5秒）
        SingleOutputStreamOperator<JSONObject> operator = kafka_source.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts_ms");// 使用 ts_ms 字段作为事件时间戳
                    }
                }));

//        kafka_source.print();
        // 过滤出 user_info 表的数据流
        SingleOutputStreamOperator<JSONObject> UserInfoDS = operator
                .filter(json -> json.getJSONObject("source").getString("table").equals("user_info"));
        // 过滤出 user_info_sup_msg 表的数据流
        SingleOutputStreamOperator<JSONObject> UserInfoSupDS = operator
                .filter(json -> json.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
        // 处理用户补充信息，提取关键字段如 uid、unit_height、weight 等
        SingleOutputStreamOperator<JSONObject> streamOperator = UserInfoSupDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject result = new JSONObject();
                if (value.containsKey("after") && value.getJSONObject("after") != null) {
                    JSONObject after = value.getJSONObject("after");
                    result.put("uid", after.getIntValue("uid"));
                    result.put("unit_height", after.getString("unit_height"));
                    result.put("weight", after.getString("weight"));
                    result.put("unit_weight", after.getString("unit_weight"));
                    result.put("height", after.getString("height"));
                    result.put("ts_ms", value.getLong("ts_ms"));
                    out.collect(result);
                }
            }
        });

//        streamOperator.print();
        // 处理用户基本信息，提取 name、nick_name、birthday 等字段
        // 并计算 age 和 decade，转换 birthday 格式，获取星座
        SingleOutputStreamOperator<JSONObject> outputStreamOperator = UserInfoDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject object = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                object.put("uid", after.getIntValue("id"));
                object.put("name", after.getString("name"));
                object.put("nick_name", after.getString("nick_name"));
                object.put("birthday", after.getString("birthday"));
                object.put("phone_num", after.getString("phone_num"));
                object.put("email", after.getString("email"));
                object.put("gender", after.getString("gender"));
                object.put("ts_ms", value.getLong("ts_ms"));
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate birthday = LocalDate.ofEpochDay(epochDay);
                        object.put("birthday", birthday.format(DateTimeFormatter.ISO_DATE));

                        int year = birthday.getYear();
                        int decadeStart = (year / 10) * 10;
                        int decade = decadeStart;
                        object.put("decade", decade);

                        LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                        int age = calculateAge(birthday, currentDate);
                        object.put("age", age);

                        String zodiac = getZodiacSign(birthday);
                        object.put("zodiac_sign", zodiac);
                    }
                }
                out.collect(object);
            }
        });

//        outputStreamOperator.print();
        // 清洗数据，确保 uid 不为空
        SingleOutputStreamOperator<JSONObject> UserinfoDs = streamOperator.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> UserinfoSupDs = outputStreamOperator.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        // 按照 uid 分组，准备进行 intervalJoin
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = UserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = UserinfoSupDs.keyBy(data -> data.getString("uid"));
//
//        keyedStreamUserInfoDs.print();
//       keyedStreamUserInfoSupDs.print();
        //  在 -60 到 +60 分钟的时间窗口内进行 intervalJoin，合并用户基本信息与补充信
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoSupDs.intervalJoin(keyedStreamUserInfoDs)
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject userInfo, JSONObject userInfoSup, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject merged = new JSONObject();
                        // 添加用户基本信息（来自 UserInfoDS）
                        merged.put("uid", userInfo.getString("uid"));
                        merged.put("name", userInfo.getString("name"));
                        merged.put("decade", userInfo.getString("decade"));
                        merged.put("birthday", userInfo.getString("birthday"));
                        merged.put("phone_num", userInfo.getString("phone_num"));
                        merged.put("email", userInfo.getString("email"));
                        merged.put("age", userInfo.getInteger("age"));
                        merged.put("zodiac_sign", userInfo.getString("zodiac_sign"));
                        merged.put("ts_ms", userInfo.getLong("ts_ms"));
                        if (userInfo != null && userInfo.containsKey("gender")) {
                            merged.put("gender", userInfo.getString("gender"));
                        } else {
                            merged.put("gender", "null");
                        }

                        // 添加用户补充信息（来自 UserInfoSupDS）
                        merged.put("unit_height", userInfoSup.getString("unit_height"));
                        merged.put("height", userInfoSup.getString("height"));
                        merged.put("weight", userInfoSup.getString("weight"));
                        merged.put("unit_weight", userInfoSup.getString("unit_weight"));

                        out.collect(merged);

                    }
                });

        processIntervalJoinUserInfo6BaseMessageDs.print();


        env.execute("DwdApp");
    }
    // 计算年龄
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }
    // 获取星座
    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
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
}
