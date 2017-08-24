package com.hroniko;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hroniko on 09.07.17.
 */
// Класс составного ключа для SecondarySort
public class ComparedKey implements WritableComparable<ComparedKey>  {

    private Text key = new Text(); // natural key, дата в текстовом формате
    private DoubleWritable comparedState = new DoubleWritable(); // secondary key, стоимость

    public Text getKey() {
        return this.key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public DoubleWritable getComparedState() {
        return this.comparedState;
    }

    public void setComparedState(DoubleWritable comparedState) {
        this.comparedState = comparedState;
    }

    public int compareTo(ComparedKey comparedKey) {
        int res = this.key.compareTo(comparedKey.key);
        if(res == 0){
            res = this.comparedState.compareTo(comparedKey.comparedState);
        }
        return res; // По возрастанию
        // return -1 * res; // По убыванию
    }

    public void write(DataOutput out) throws IOException {
        this.key.write(out);
        this.comparedState.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.key.readFields(in);
        this.comparedState.readFields(in);
    }

}