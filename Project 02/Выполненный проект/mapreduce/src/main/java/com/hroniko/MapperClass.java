package com.hroniko;

import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * Created by Hroniko on 06.07.2017.
 */
// Класс-маппер минимальной цены с начальными ключем-значением и выходными ключем-значением
// Начальный ключ должен быть Writable или WritableComparable для того, чтобы была возможна сериализация
public class MapperClass extends org.apache.hadoop.mapreduce.Mapper<Text, Text, ComparedKey, DoubleWritable> {


    @Override
    // Превращаем строку (значение) в ключ-значение (дата, цена)
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 0 Забираем строку
        String line = value.toString();

        /* Старый способ через substring
        String str_date = line.substring(0, 10); // Забираем дату-время в виде строки
        str_date = DateConverter.textToDateText(str_date); // и конвертируем к нужному формату yyyyMM

        // 2 Работа с ценой
        Double price = Double.parseDouble(line.substring(20, line.length()-1)); // Вытаскиваем всю оставшуюся подстроку и конвертируем ее к даблу

        */

        // 1 Работа с датой:
        String str[] = line.split(";"); // Разбираем строку на компоненты через split по разделителю ;
        String str_date = DateConverter.textToDateText(str[0].trim()); // и конвертируем к нужному формату yyyyMM

        // 2 Работа с ценой
        Double price = Double.parseDouble(str[1].trim()); // Вытаскиваем вторую подстроку и конвертируем ее к даблу

        // 3 Сохраняем составной ключ (дата, сумма) и значение (сумма)
        ComparedKey ck = new ComparedKey(); // ComparedKey ck = new ComparedKey(new Text(str_date), new DoubleWritable(price)); // Через конструктор в ComparedKey не пошло...
        ck.setKey(new Text(str_date));
        ck.setComparedState(new DoubleWritable(price));

        context.write(ck, new DoubleWritable(price)); // context.write(new Text(str_date), new DoubleWritable(price));
    }
}
