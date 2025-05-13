package com.slh.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @Package com.slh.utils.HBaseUtil1
 * @Author song.lihao
 * @Date 2025/5/12 22:40
 * @description:
 */
public class HBaseUtil1 {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    private static Connection connection = null;
    private static Admin admin = null;

    // 私有构造方法，防止实例化
    private HBaseUtil1() {}

    /**
     * 获取HBase连接
     * @return HBase连接对象
     */
    public static synchronized Connection getHBaseConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                Configuration config = HBaseConfiguration.create();
                // 设置HBase配置参数
                config.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
                config.set("hbase.zookeeper.property.clientPort", "2181");
                config.set("hbase.client.retries.number", "3");
                config.set("hbase.client.pause", "1000");
                config.set("hbase.rpc.timeout", "5000");
                config.set("hbase.client.operation.timeout", "10000");
                config.set("hbase.client.scanner.timeout.period", "60000");

                connection = ConnectionFactory.createConnection(config);
                logger.info("HBase连接创建成功");
            } catch (IOException e) {
                logger.error("获取HBase连接失败", e);
                throw new RuntimeException("获取HBase连接失败", e);
            }
        }
        return connection;
    }

    /**
     * 获取HBase Admin对象
     * @return HBase Admin对象
     */
    public static synchronized Admin getHBaseAdmin() {
        if (admin == null || admin.isAborted()) {
            try {
                admin = getHBaseConnection().getAdmin();
                logger.info("HBase Admin获取成功");
            } catch (IOException e) {
                logger.error("获取HBase Admin失败", e);
                throw new RuntimeException("获取HBase Admin失败", e);
            }
        }
        return admin;
    }

    /**
     * 关闭HBase连接
     */
    public static synchronized void closeHBaseConnection() {
        if (admin != null) {
            try {
                admin.close();
                admin = null;
                logger.info("HBase Admin关闭成功");
            } catch (IOException e) {
                logger.error("关闭HBase Admin失败", e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
                connection = null;
                logger.info("HBase连接关闭成功");
            } catch (IOException e) {
                logger.error("关闭HBase连接失败", e);
            }
        }
    }

    /**
     * 关闭指定的HBase连接
     * @param conn 要关闭的连接
     */
    public static void closeHBaseConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                logger.info("指定的HBase连接关闭成功");
            } catch (IOException e) {
                logger.error("关闭指定的HBase连接失败", e);
            }
        }
    }

    /**
     * 创建HBase表
     * @param conn HBase连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @param columnFamilies 列族数组
     */
    public static void createHBaseTable(Connection conn, String namespace, String tableName, String... columnFamilies) {
        try (Admin admin = conn.getAdmin()) {
            // 构建表名
            TableName tn = TableName.valueOf(namespace + ":" + tableName);

            if (admin.tableExists(tn)) {
                logger.warn("表 {} 已存在，无需创建", tn);
                return;
            }

            // 创建表描述器
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tn);

            // 添加列族
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>();
            for (String family : columnFamilies) {
                familyDescriptors.add(ColumnFamilyDescriptorBuilder.of(family));
            }

            tableDescriptor.setColumnFamilies(familyDescriptors);

            // 创建表
            admin.createTable(tableDescriptor.build());
            logger.info("表 {} 创建成功", tn);
        } catch (IOException e) {
            logger.error("创建表 {} 失败", tableName, e);
            throw new RuntimeException("创建HBase表失败", e);
        }
    }

    /**
     * 删除HBase表
     * @param conn HBase连接
     * @param namespace 命名空间
     * @param tableName 表名
     */
    public static void dropHBaseTable(Connection conn, String namespace, String tableName) {
        try (Admin admin = conn.getAdmin()) {
            TableName tn = TableName.valueOf(namespace + ":" + tableName);

            if (!admin.tableExists(tn)) {
                logger.warn("表 {} 不存在，无需删除", tn);
                return;
            }

            // 禁用表
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }

            // 删除表
            admin.deleteTable(tn);
            logger.info("表 {} 删除成功", tn);
        } catch (IOException e) {
            logger.error("删除表 {} 失败", tableName, e);
            throw new RuntimeException("删除HBase表失败", e);
        }
    }

    /**
     * 检查表是否存在
     * @param conn HBase连接
     * @param namespace 命名空间
     * @param tableName 表名
     * @return 是否存在
     */
    public static boolean tableExists(Connection conn, String namespace, String tableName) {
        try (Admin admin = conn.getAdmin()) {
            TableName tn = TableName.valueOf(namespace + ":" + tableName);
            return admin.tableExists(tn);
        } catch (IOException e) {
            logger.error("检查表是否存在失败", e);
            throw new RuntimeException("检查表是否存在失败", e);
        }
    }

    /**
     * 向HBase表中插入数据
     * @param conn HBase连接
     * @param tableName 表名
     * @param put Put对象
     */
    public static void putRow(Connection conn, String tableName, Put put) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.put(put);
            logger.debug("数据插入成功，rowKey: {}", Bytes.toString(put.getRow()));
        } catch (IOException e) {
            logger.error("插入数据失败", e);
            throw new RuntimeException("插入数据失败", e);
        }
    }

    /**
     * 批量插入数据到HBase表
     * @param conn HBase连接
     * @param tableName 表名
     * @param puts Put对象列表
     */
    public static void putRows(Connection conn, String tableName, List<Put> puts) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.put(puts);
            logger.info("批量插入 {} 条数据成功", puts.size());
        } catch (IOException e) {
            logger.error("批量插入数据失败", e);
            throw new RuntimeException("批量插入数据失败", e);
        }
    }

    /**
     * 根据rowKey获取数据
     * @param conn HBase连接
     * @param tableName 表名
     * @param rowKey rowKey
     * @return Result对象
     */
    public static Result getRow(Connection conn, String tableName, String rowKey) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            logger.error("获取数据失败", e);
            throw new RuntimeException("获取数据失败", e);
        }
    }

    /**
     * 扫描表数据
     * @param conn HBase连接
     * @param tableName 表名
     * @param startRow 起始rowKey
     * @param stopRow 结束rowKey
     * @return ResultScanner对象
     */
    public static ResultScanner scanTable(Connection conn, String tableName, String startRow, String stopRow) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            if (startRow != null) {
                scan.withStartRow(Bytes.toBytes(startRow));
            }
            if (stopRow != null) {
                scan.withStopRow(Bytes.toBytes(stopRow));
            }
            return table.getScanner(scan);
        } catch (IOException e) {
            logger.error("扫描表数据失败", e);
            throw new RuntimeException("扫描表数据失败", e);
        }
    }

    /**
     * 删除行数据
     * @param conn HBase连接
     * @param tableName 表名
     * @param rowKey rowKey
     */
    public static void deleteRow(Connection conn, String tableName, String rowKey) {
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            logger.info("删除行数据成功，rowKey: {}", rowKey);
        } catch (IOException e) {
            logger.error("删除行数据失败", e);
            throw new RuntimeException("删除行数据失败", e);
        }
    }

    /**
     * 创建命名空间
     * @param conn HBase连接
     * @param namespace 命名空间名称
     */
    public static void createNamespace(Connection conn, String namespace) {
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
            admin.createNamespace(builder.build());
            logger.info("命名空间 {} 创建成功", namespace);
        } catch (NamespaceExistException e) {
            logger.warn("命名空间 {} 已存在", namespace);
        } catch (IOException e) {
            logger.error("创建命名空间 {} 失败", namespace, e);
            throw new RuntimeException("创建命名空间失败", e);
        }
    }

    /**
     * 检查命名空间是否存在
     * @param conn HBase连接
     * @param namespace 命名空间名称
     * @return 是否存在
     */
    public static boolean namespaceExists(Connection conn, String namespace) {
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor ns : namespaces) {
                if (ns.getName().equals(namespace)) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            logger.error("检查命名空间是否存在失败", e);
            throw new RuntimeException("检查命名空间是否存在失败", e);
        }
    }
}
