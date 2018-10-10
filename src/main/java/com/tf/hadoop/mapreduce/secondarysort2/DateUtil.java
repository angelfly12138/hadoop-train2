package com.tf.hadoop.mapreduce.secondarysort2;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 定义一些有用的日期处理方法
 */

public class DateUtil {
    static final String DATE_FORMAT = "yyyy-MM-dd";
    static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT);

    public static Date getDate(String dateAsString)  {
        try {
            return SIMPLE_DATE_FORMAT.parse(dateAsString);
        }
        catch(Exception e) {
            return null;
        }
    }

    public static long getDateAsMilliSeconds(Date date) throws Exception {
        return date.getTime();
    }

    public static long getDateAsMilliSeconds(String dateAsString) throws Exception {
        Date date = getDate(dateAsString);
        return date.getTime();
    }

    public static String getDateAsString(long timestamp) {
        return SIMPLE_DATE_FORMAT.format(timestamp);
    }
}
