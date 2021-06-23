package com.moyu.flink.examples.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 *     日期转换工具
 */

public class DateUtils {

    private static ThreadLocal<DateFormat> formatThreadLocal = new ThreadLocal<DateFormat>(){
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static ThreadLocal<DateFormat> dateFormatThreadLocal = new ThreadLocal<DateFormat>(){
        @Override
        protected DateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };


    public static long strToTimestamp(String createtime) throws ParseException {
        Date date = formatThreadLocal.get().parse(createtime);
        long timestamp = date.getTime();
        return timestamp;
    }




}
