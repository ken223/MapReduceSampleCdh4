package com.example.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSample extends Mapper<Text, Text, Text, Text> {
    // a short description of MapperTest
    // input -> MapperTest -> <K, V>
    // K: contents of input(no split, no change)
    // V: 0
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] elem = value.toString().split(",");
        String mapkey = elem[1] + "-" + elem[2];

        context.write(new Text(mapkey), value);
    }
}