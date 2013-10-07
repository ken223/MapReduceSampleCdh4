package com.example.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSample extends Mapper<LongWritable, Text, Text, Text> {
    // a short description of MapperTest
    // input -> MapperTest -> <K, V>
    // K: contents of input(no split, no change)
    // V: 0
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] elem = value.toString().split(",");
        // String mapkey = elem[0] + "-" + elem[1];
        String mapkey = elem[0];

        context.write(new Text(mapkey), value);
    }
}