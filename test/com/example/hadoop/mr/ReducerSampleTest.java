package com.example.hadoop.mr;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ReducerSampleTest {
    @Test
    public void test_reduce() {
        List<Text> pairList = new ArrayList<Text>();
        pairList.add(new Text("a,b,c,d,e,f,g"));
        pairList.add(new Text("a,b,c,d,e,f,g"));

        try {
            new ReduceDriver<Text, Text, Text, LongWritable>().withReducer(new ReducerSample())
                    .withInput(new Text("a"), pairList).withOutput(new Text("a"), new LongWritable(2)).runTest();
        } catch (IOException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
            fail("error!!");
        }

    }

}
