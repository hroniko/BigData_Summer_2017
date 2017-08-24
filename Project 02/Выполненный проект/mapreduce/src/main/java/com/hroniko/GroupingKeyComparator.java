package com.hroniko;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by hroniko on 09.07.17.
 */
// Класс для сравнения уже самих данных, которые приходят на редьюсер,
// т.е. данные того типа, который мы написали
public class GroupingKeyComparator extends WritableComparator {
    public GroupingKeyComparator() {
        super(ComparedKey.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        ComparedKey val1 = (ComparedKey)w1;
        ComparedKey val2 = (ComparedKey)w2;
        return val1.getKey().compareTo(val2.getKey());
    }
}