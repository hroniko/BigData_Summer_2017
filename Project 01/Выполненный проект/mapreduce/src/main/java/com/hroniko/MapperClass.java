package com.hroniko;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Hroniko on 06.07.2017.
 */
// Класс-маппер минимальной цены
public class MapperClass extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, DoubleWritable> {


    @Override
    // Превращаем строку (значение) в ключ-значение (дата, цена)
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 1 Работа с датой
        String line = value.toString(); // Забираем строку
        String str_date = line.substring(0, 10); // Забираем дату-время в виде строки
        str_date = DateConverter.textToDateText(str_date); // и конвертируем к нужному формату yyyyMM

        // 2 Работа с ценой
        Double price = Double.parseDouble(line.substring(20, line.length()-1)); // Вытаскиваем всю оставшуюся подстроку и конвертируем ее к даблу

        // 3 Сохраняем
        context.write(new Text(str_date), new DoubleWritable(price));
    }
}
