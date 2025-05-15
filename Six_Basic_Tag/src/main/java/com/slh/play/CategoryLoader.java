package com.slh.play;

import com.slh.function.DimBaseCategory;
import com.slh.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @Package com.slh.play.CategoryLoader
 * @Author song.lihao
 * @Date 2025/5/15 18:39
 * @description:
 */
public class CategoryLoader {
    private static List<DimBaseCategory> dimBaseCategories;

    static {
        try (Connection conn = DriverManager.getConnection(Config.DB_URL, Config.DB_USER, Config.DB_PASSWORD)) {
            String sql = "SELECT b3.id, b3.name as b3name, b2.name as b2name, b1.name as b1name " +
                    "FROM base_category3 b3 " +
                    "JOIN base_category2 b2 ON b3.category2_id = b2.id " +
                    "JOIN base_category1 b1 ON b2.category1_id = b1.id";
            dimBaseCategories = JdbcUtils.queryList2(conn, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException("初始化分类数据失败", e);
        }
    }

    public static List<DimBaseCategory> getCategories() {
        return dimBaseCategories;
    }
}
