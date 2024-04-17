package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        double avgPrecipitation = 0;
        int count = 0;
        for(DoubleWritable value: values){
            avgPrecipitation += value.get();
            count++;
        }
        avgPrecipitation = avgPrecipitation / count;
        context.write(key, new DoubleWritable(avgPrecipitation));
    }
}