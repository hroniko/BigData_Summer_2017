package com.hroniko;

import org.apache.hadoop.io.*;

/**
 * Created by hroniko on 09.07.17.
 */
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator() {
        super(ComparedKey.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        ComparedKey compositeKey1 = (ComparedKey)w1;
        ComparedKey compositeKey2 = (ComparedKey)w2;
        Text ctnKey1 = compositeKey1.getKey();
        Text ctnKey2 = compositeKey2.getKey();
        DoubleWritable comparedState1 = compositeKey1.getComparedState();
        DoubleWritable comparedState2 = compositeKey2.getComparedState();
        int comp = ctnKey1.compareTo(ctnKey2);
        return comp == 0 ? comparedState1.compareTo(comparedState2) : comp;
    }
}