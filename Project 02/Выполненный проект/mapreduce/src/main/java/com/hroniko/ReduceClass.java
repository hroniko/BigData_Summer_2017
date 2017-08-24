package com.hroniko;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Hroniko on 06.07.2017.
 */
// Входные ключ-значения те же, что и выходные ключ-значение у маппера, а выходные ключ-значение - NullWritable и Text соответственно
public class ReduceClass extends Reducer<ComparedKey, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(ComparedKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        // 1
        Text text_key = key.getKey();
        String res = text_key.toString();

        // 2 Обходим коллекцию и конкатенируем все
        for (DoubleWritable value : values){

            res += " ; " + String.format("%(.2f", value.get());
        }
        // 3 Запоминаем
        context.write(new Text(text_key), new Text(res));
    }
}
