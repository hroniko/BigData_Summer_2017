package com.hroniko;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hroniko on 09.07.17.
 */

// Класс-распределитель данных по редьюсерам (берем хэш-код от ключа и делим на количество партиций)
public class DateCountPartitioner extends Partitioner<ComparedKey, DoubleWritable> {
    @Override
    public int getPartition(ComparedKey comparedKey, DoubleWritable value, int numberOfPartitions) {
        return Math.abs(comparedKey.getComparedState().hashCode() % numberOfPartitions);
    }
}
