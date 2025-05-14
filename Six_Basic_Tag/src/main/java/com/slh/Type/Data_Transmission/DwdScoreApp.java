package com.slh.Type.Data_Transmission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.function.DimBaseCategory;
import com.slh.function.DimCategoryCompare;
import com.slh.utils.FlinkSourceUtil;
import com.slh.utils.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.lzy.app.dwd.DwdScoreApp
 * @Author zheyuan.liu
 * @Date 2025/5/14 14:46
 * @description:
 */

public class DwdScoreApp {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 从 kafka 获取日志数据源
        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("topic_log", "page_Log");

        SingleOutputStreamOperator<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 将 JSON 字符串转换为 JSONObject
        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject);
        // 使用 RichMapFunction 进行数据转换和评分计算
        SingleOutputStreamOperator<JSONObject> operator = streamOperatorlog.map(new RichMapFunction<JSONObject, JSONObject>() {

            private List<DimBaseCategory> dim_base_categories;// 基础分类维度数据
            private Map<String, DimBaseCategory> categoryMap;// 分类名称到分类对象的映射
            private List<DimCategoryCompare> dimCategoryCompares;// 分类比较维度数据
            private Connection connection;// 数据库连接

            final double deviceRate = 0.1;// 设备类型权重
            final double searchRate = 0.15;// 搜索行为权重
            final double timeRate = 0.1;// 时间权重

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化 Map
                categoryMap = new HashMap<>();

                // 获取数据库连接
                connection = JdbcUtil.getMySQLConnection();
                // 查询三级分类数据
                String sql1 = "select b3.id,                          \n" +
                        "            b3.name as b3name,              \n" +
                        "            b2.name as b2name,              \n" +
                        "            b1.name as b1name               \n" +
                        "     from dev_realtime_v1.base_category3 as b3  \n" +
                        "     join dev_realtime_v1.base_category2 as b2  \n" +
                        "     on b3.category2_id = b2.id             \n" +
                        "     join dev_realtime_v1.base_category1 as b1  \n" +
                        "     on b2.category1_id = b1.id";
                dim_base_categories = JdbcUtil.queryList(connection, sql1, DimBaseCategory.class, false);
                // 查询分类比较字典数据
                String sql2 = "select id, category_name, search_category from dev_realtime_v1.category_compare_dic;";
                dimCategoryCompares = JdbcUtil.queryList(connection, sql2, DimCategoryCompare.class, false);

                // 在 open 方法中初始化 categoryMap
                for (DimBaseCategory category : dim_base_categories) {
                    categoryMap.put(category.getB3name(), category);
                }

                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) {
                // 处理操作系统的字段
                String os = jsonObject.getString("os");
                // 判断 os 是否为空
                if (os == null || os.isEmpty()) {
                    // 可选：记录日志、输出到侧输出流等
                    return jsonObject; // 或 throw new RuntimeException("os is null or empty");
                }
                // 取第一个操作系统标签作为判断依据
                String[] labels = os.split(",");
                if (labels.length == 0) {
                    return jsonObject;
                }

                String judge_os = labels[0];
                jsonObject.put("judge_os", judge_os);
                // 根据操作系统类型计算设备评分
                if (judge_os.equals("iOS")) {
                    // IOS 设备各年龄段评分
                    jsonObject.put("device_18_24", round(0.7 * deviceRate));
                    jsonObject.put("device_25_29", round(0.6 * deviceRate));
                    jsonObject.put("device_30_34", round(0.5 * deviceRate));
                    jsonObject.put("device_35_39", round(0.4 * deviceRate));
                    jsonObject.put("device_40_49", round(0.3 * deviceRate));
                    jsonObject.put("device_50", round(0.2 * deviceRate));
                } else if (judge_os.equals("Android")) {
                    // Android 设备各年龄段评分
                    jsonObject.put("device_18_24", round(0.8 * deviceRate));
                    jsonObject.put("device_25_29", round(0.7 * deviceRate));
                    jsonObject.put("device_30_34", round(0.6 * deviceRate));
                    jsonObject.put("device_35_39", round(0.5 * deviceRate));
                    jsonObject.put("device_40_49", round(0.4 * deviceRate));
                    jsonObject.put("device_50", round(0.3 * deviceRate));
                }
                // 处理搜索词分类
                String searchItem = jsonObject.getString("search_item");
                if (searchItem != null && !searchItem.isEmpty()) {
                    // 根据搜索词查找对应的分类
                    DimBaseCategory category = categoryMap.get(searchItem);
                    if (category != null) {
                        jsonObject.put("b1_category", category.getB1name());
                    }
                }
                // 根据一级分类查找搜索分类
                String b1Category = jsonObject.getString("b1_category");
                if (b1Category != null && !b1Category.isEmpty()) {
                    for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                        if (b1Category.equals(dimCategoryCompare.getCategoryName())) {
                            jsonObject.put("searchCategory", dimCategoryCompare.getSearchCategory());
                            break;
                        }
                    }
                }
                // 根据搜索分类计算搜索评分
                String searchCategory = jsonObject.getString("searchCategory");
                if (searchCategory == null) {
                    searchCategory = "unknown";
                }
                // 不同搜索分类对应不同年龄段的评分
                switch (searchCategory) {
                    case "时尚与潮流":
                        jsonObject.put("search_18_24", round(0.9 * searchRate));
                        jsonObject.put("search_25_29", round(0.7 * searchRate));
                        jsonObject.put("search_30_34", round(0.5 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "性价比":
                        jsonObject.put("search_18_24", round(0.2 * searchRate));
                        jsonObject.put("search_25_29", round(0.4 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.8 * searchRate));
                        break;
                    case "健康与养生":
                        jsonObject.put("search_18_24", round(0.1 * searchRate));
                        jsonObject.put("search_25_29", round(0.2 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.6 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.9 * searchRate));
                        break;
                    case "家庭与育儿":
                        jsonObject.put("search_18_24", round(0.1 * searchRate));
                        jsonObject.put("search_25_29", round(0.2 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.6 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    case "科技与数码":
                        jsonObject.put("search_18_24", round(0.8 * searchRate));
                        jsonObject.put("search_25_29", round(0.6 * searchRate));
                        jsonObject.put("search_30_34", round(0.4 * searchRate));
                        jsonObject.put("search_35_39", round(0.3 * searchRate));
                        jsonObject.put("search_40_49", round(0.2 * searchRate));
                        jsonObject.put("search_50", round(0.1 * searchRate));
                        break;
                    case "学习与发展":
                        jsonObject.put("search_18_24", round(0.4 * searchRate));
                        jsonObject.put("search_25_29", round(0.5 * searchRate));
                        jsonObject.put("search_30_34", round(0.6 * searchRate));
                        jsonObject.put("search_35_39", round(0.7 * searchRate));
                        jsonObject.put("search_40_49", round(0.8 * searchRate));
                        jsonObject.put("search_50", round(0.7 * searchRate));
                        break;
                    default:
                        // 未知分类默认评分为0
                        jsonObject.put("search_18_24", 0);
                        jsonObject.put("search_25_29", 0);
                        jsonObject.put("search_30_34", 0);
                        jsonObject.put("search_35_39", 0);
                        jsonObject.put("search_40_49", 0);
                        jsonObject.put("search_50", 0);
                }


                return jsonObject;
            }
            // 四舍五入保留3位小数
            private double round(double value) {
                return BigDecimal.valueOf(value)
                        .setScale(3, RoundingMode.HALF_UP)
                        .doubleValue();
            }

            @Override
            public void close() throws Exception {
                // 关闭数据库连接
                super.close();
            }
        });

        operator.print();


        env.execute("minutes_page_Log");
    }
}