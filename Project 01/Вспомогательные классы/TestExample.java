import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Created by MikhYuMikhaylov on 05.07.2017.
 */
public class TestExample {
    public static MultipleInputsMapReduceDriver<K1, V1, K2, V2> mapReduceDriver;
    MapperClass mapper = new MapperClass();

    @Before
    public void setup(){
        Configuration conf = new Configuration();
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        mapReduceDriver.setReducer(new ReduceClass());
        mapReduceDriver.addMapper(mapper);
        mapReduceDriver.setConfiguration(conf);
    }

    @Test
    public void test() throws IOException {
        Configuration conf = new Configuration();
        mapReduceDriver.setConfiguration(conf);
        String inputPath = "";
        String outputPath = "";
        TestUtils.loadInputText(inputPath, mapReduceDriver, mapper);
        List<Pair<NullWritable, Text>> out = mapReduceDriver.run();
        List<String> res = TestUtils.getStringListFromValues(out);
        TestUtils.listOfStringsToCsv(res, outputPath);
    }
}
