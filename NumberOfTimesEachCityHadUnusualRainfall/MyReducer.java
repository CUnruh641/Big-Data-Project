package com.mycompany.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Haebin Noh
// The number of times each city had unusual rainfall

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        double sum = 0;
        int count = 0;
        List<Double> precipitationValues = new ArrayList<>();

        for(DoubleWritable value: values){
            sum += value.get();
            count++;
            precipitationValues.add(value.get());
        }
        
        double average = sum / count;
        
        int unusualCount = 0;
        for (double value : precipitationValues) {
            if (value > average) {
                unusualCount++;
            }
        }
        
        context.write(key, new DoubleWritable(unusualCount));
    }
}
