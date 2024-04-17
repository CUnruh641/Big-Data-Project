package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.util.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ajay
 */
public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        double avg_precip = 0.0;
        int count = 0;
        Double max = 0.0;
        

        for(DoubleWritable value: values){
           avg_precip += value.get();
           count += 1;
           if(count == 1){
            max = value.get();
           }
           else if(value.get() > max){
            max = value.get();
           }
        }
        if (count != 0){
        avg_precip = avg_precip / count;
        }

        if(count != 0){
        context.write(key, new DoubleWritable(avg_precip));
        context.write(key, new DoubleWritable(max));
        }

    }
}