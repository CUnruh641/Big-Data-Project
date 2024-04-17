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
        double avg_temp = 0.0;
        int count = 0;
        HashMap<Double, Integer> avg_temp_mode = new HashMap<Double, Integer>();
        double mode = 0.0;
        int modeCount = 0;
        Double range = 0.0;
        Double max = 0.0;
        Double min = 0.0;

        for(DoubleWritable value: values){
           avg_temp += value.get();
           count += 1;
           if(count == 1){
            max = value.get();
            min = value.get();
           }
           else if(value.get() > max){
            max = value.get();
           }
           else if(value.get() < min){
            min = value.get();
           }
           if(avg_temp_mode.containsKey(value.get())){
                avg_temp_mode.put(value.get(), avg_temp_mode.get(value.get()) + 1);
           }
           else{
                avg_temp_mode.put(value.get(), 1);
           }
        }
        if (count != 0){
        avg_temp = avg_temp / count;
        }
        range = max - min;
        for(Double i: avg_temp_mode.keySet()){
            if(modeCount == 0){
                mode = i;
                modeCount = avg_temp_mode.get(i);
            }

            else if(avg_temp_mode.get(i) > modeCount){
                mode = i;
                modeCount = avg_temp_mode.get(i);
            }
        }

        if(count != 0){
        context.write(key, new DoubleWritable(avg_temp));
        context.write(key, new DoubleWritable(mode));
        context.write(key, new DoubleWritable(range));
        }

    }
}