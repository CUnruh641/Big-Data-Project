package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Haebin Noh
// The average snow depth for winter season for each city

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        double temp = 0;
        int count = 0;

        for(DoubleWritable value: values){
            temp += value.get();
            count += 1;
        }

        context.write(key, new DoubleWritable(temp/count));
    }
}
