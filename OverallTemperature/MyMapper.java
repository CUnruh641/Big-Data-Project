package com.mycompany.app;

import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajay
 * Modified by: Nathan Eloe
 * 
 */

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String[] row = value.toString().split(",");

        if(!row[4].isEmpty()){
        context.write(new Text(row[1]), new DoubleWritable(Double.parseDouble(row[4])));
        }
        
    }
    
}