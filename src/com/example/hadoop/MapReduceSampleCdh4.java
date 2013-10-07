package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.hadoop.mr.MapperSample;
import com.example.hadoop.mr.ReducerSample;

public class MapReduceSampleCdh4 extends Configured implements Tool {

    /**
     * Main method. Start ToolRunner#run()
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MapReduceSampleCdh4(), args);
        System.exit(res);
    }

    @Override
    public Configuration getConf() {
        return new Configuration(true);
    }

    @Override
    public void setConf(Configuration arg0) {
    }

    @Override
    public int run(final String[] args) throws Exception {
        // get application specific args.
        String[] appArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        // check my application arguments.
        if (appArgs.length != 2) {
            System.err.println("Usage: HelloMapReduce <in> <out>");
            System.exit(2);
        }

        Path inputPath = new Path(appArgs[0]);
        Path outputPath = new Path(appArgs[1]);

        // Create a Job using the processed conf and set JobName with jobName.
        String jobName = "SampleMapReduce";
        Job job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(getClass());

        // Specify various job-specific parameters.
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(MapReduceSampleCdh4.class);
        job.setMapperClass(MapperSample.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // misc job configurations.
        job.setReducerClass(ReducerSample.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Submit the job, then poll for progress until the job is complete
        // if do not care about the progress, job.waitForCompletion(false).
        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
