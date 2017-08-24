package com.hroniko;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MikhYuMikhaylov on 05.07.2017.
 */

// Класс для чтения и записи
public class TestUtils {

    public static void loadInputText(String inputPath,
                                     MultipleInputsMapReduceDriver mapReduceDriver,
                                     Mapper mapper) throws IOException {
        WritableComparable KEY = new Text();
        BufferedReader reader = new BufferedReader(new FileReader(inputPath));
        List<Pair<WritableComparable, Text>> input = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            input.add(new Pair<>(KEY, new Text(line)));
        }
        reader.close();
        mapReduceDriver.addAll(mapper, input);
    }

    public static List<String> getStringListFromValues(List<?> pairList) {
        List<String> out = new ArrayList<>();
        for (Pair p : (List<Pair<?, ?>>) pairList) {
            out.add(p.getSecond().toString());
        }
        return out;
    }

    public static void listOfStringsToCsv(List<String> list, String path) throws FileNotFoundException {
        PrintWriter pw = new PrintWriter(path);
        for (String st : list) {
            pw.write(st + "\n");
        }
        pw.flush();
        pw.close();
    }



}
