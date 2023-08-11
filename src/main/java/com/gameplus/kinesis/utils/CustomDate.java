package com.gameplus.kinesis.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CustomDate {
    public static String normalize(String dateStr, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

//Date对象转换为字符串输出
        LocalDateTime localDate = LocalDateTime.parse(dateStr, formatter);

        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(localDate);
    }
}
