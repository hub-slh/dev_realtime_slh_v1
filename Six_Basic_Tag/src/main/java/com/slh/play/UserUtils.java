package com.slh.play;

import java.time.LocalDate;
import java.time.Period;

/**
 * @Package com.slh.play.UserUtils
 * @Author song.lihao
 * @Date 2025/5/15 18:38
 * @description:
 */
public class UserUtils {
    /**
     * 计算年龄
     */
    public static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    /**
     * 计算星座
     */
    public static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();
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
