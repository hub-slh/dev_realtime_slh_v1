package com.slh.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Package com.slh.utils.DateTimeUtils
 * @Author song.lihao
 * @Date 2025/5/18 19:21
 * @description:
 */
public class DateTimeUtils {
    public final static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
        private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        public static String format(Date date) {
            return format(date, YYYY_MM_DD_HH_MM_SS);
        }

        public static String format(Date date, String format) {
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            return formatter.format(date);
        }
        public static String tsToDate(Long ts) {
            Date dt = new Date(ts);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
            return dtf.format(localDateTime);
        }
}
