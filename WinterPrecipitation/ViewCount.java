package com.mycompany.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ViewCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        Job job = Job.getInstance(conf);
        job.setJobName("ViewCount");
        job.setJarByClass(ViewCount.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new ViewCount(), args);
        System.exit(exitCode);
    }

}
