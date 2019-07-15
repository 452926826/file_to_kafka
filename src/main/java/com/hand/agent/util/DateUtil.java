package com.hand.agent.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtil {
    public static String getPOIDate(boolean use1904windowing, double value) {
        int wholeDays = (int) Math.floor(value);
        int millisecondsInDay = (int) ((value - (double) wholeDays) * 8.64E7D + 0.5D);
        Calendar calendar = new GregorianCalendar();
        short startYear = 1900;
        byte dayAdjust = -1;
        if (use1904windowing) {
            startYear = 1904;
            dayAdjust = 1;
        } else if (wholeDays < 61) {
            dayAdjust = 0;
        }
        calendar.set(startYear, 0, wholeDays + dayAdjust, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, millisecondsInDay);
        if (calendar.get(Calendar.MILLISECOND) == 0) {
            calendar.clear(Calendar.MILLISECOND);
        }
        Date date = calendar.getTime();
        SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH：mm：ss");
        return s.format(date);
    }
}
