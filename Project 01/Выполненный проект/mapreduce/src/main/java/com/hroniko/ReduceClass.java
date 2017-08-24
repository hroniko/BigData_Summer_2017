package com.hroniko;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Hroniko on 06.07.2017.
 */
public class ReduceClass extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        Double maxValue = 0.0; // В самом начале максимальной суммой продажи делаем ноль, так как отрицательной цены быть не может
        Double minValue = Double.MAX_VALUE; // А минимальной суммой - макимально допустимое значение типа Double

        // 1 Обходим коллекцию и находим максимальный и минимальный элемент
        for (DoubleWritable value : values){
            maxValue = Math.max(maxValue, value.get());
            minValue = Math.min(minValue, value.get());
        }
        // 2 Преобразуем к строке вида MIN_PRICE, MAX_PRICE  (с округлением до 2х знаков после запятой)
        String min_max_res = key.toString() + ", " + String.format("%(.2f", minValue) + ", " + String.format("%(.2f", maxValue);

        // 3 Запоминаем
        context.write(key, new Text(min_max_res));
    }
}
