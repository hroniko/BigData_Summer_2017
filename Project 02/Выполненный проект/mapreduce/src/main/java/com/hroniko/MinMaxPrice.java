package com.hroniko;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Hroniko on 06.07.2017.
 */
// Класс для определения минимальной и максимальной цены за каждый месяц в наборе данных
public class MinMaxPrice {

    public static void main(String[] args) throws Exception {
        /*if (args.length != 2){
            System.err.println("Usage: MinMaxPrice <input path> <output path>");
            System.exit(-1);
        }
        */

        Job job = Job.getInstance(); // new Job();
        job.setJarByClass(MinMaxPrice.class);
        job.setJobName("MinMaxPrice");

        FileInputFormat.addInputPath(job, new Path("D:\\1\\1.csv")); // FileInputFormat.addInputPath(job, new Path("file://D:/1/1.csv")); // FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("D:\\1\\2.txt")); // FileOutputFormat.setOutputPath(job, new Path("file://D:/1/2.csv")); // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 2017-07-09
        job.setPartitionerClass(DateCountPartitioner.class);
        job.setGroupingComparatorClass(CompositeKeyComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
