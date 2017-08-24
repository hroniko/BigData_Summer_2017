package com.hroniko;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by Hroniko on 06.07.2017.
 */
public class DateConverter {

    // Превращает строку вида 2016-07-05 00:00:00 в дату joda.time
    public static DateTime toDate(String str_date){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd"); // DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        return formatter.parseDateTime(str_date);
    }

    // Конвертирует DateTime в строку формата yyyyMM
    public static String toText(DateTime dt){
        DateTimeFormatter formatter  = DateTimeFormat.forPattern("yyyyMM");
        return dt.toString(formatter);
    }

    // Полный конвертер
    public static String textToDateText(String str_date){
        return toText(toDate(str_date));
    }


}
