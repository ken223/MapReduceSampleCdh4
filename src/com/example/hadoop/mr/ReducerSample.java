package com.example.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSample extends Reducer<Text, Text, Text, LongWritable> {

    // public void setup(Context context) {
    // }

    // for drop value
    // private final static Text nullText = new Text("");
    // a short description of ReducerTest
    // output K and drop V
    // <K, V> -> ReducerTest -> K
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // キーごと割り振られたvalueを処理する
        long count = 0;
        for (Text value : values) {
            // context.write(key, values.next());
            count++;
        }
        context.write(key, new LongWritable(count));
    }
    // protected void cleanup(Context context) throws IOException,
    // InterruptedException {
    // }
}
