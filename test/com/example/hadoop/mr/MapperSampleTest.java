package com.example.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.example.hadoop.mr.MapperSample;

public class MapperSampleTest {

    @Test
    public void test_map() throws Exception {

        new MapDriver<LongWritable, Text, Text, Text>().withMapper(new MapperSample())
                .withInput(new LongWritable(4), new Text("a,b,c,d,e,f,g"))
                .withOutput(new Text("a"), new Text("a,b,c,d,e,f,g")).runTest();

    }

}
