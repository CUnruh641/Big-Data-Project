package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

// Haebin Noh
// Range of highest and lowest temperatures for each season in each city

public class MyReducer extends Reducer<Text, Text, Text, DoubleWritable>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        double maxTemp = Double.MIN_VALUE;
        double minTemp = Double.MAX_VALUE;

        for (Text value : values) {
            String[] temperatures = value.toString().split(",");
            double maxTemperature = Double.parseDouble(temperatures[0]);
            double minTemperature = Double.parseDouble(temperatures[1]);

            if (maxTemperature > maxTemp) {
                maxTemp = maxTemperature;
            }

            if (minTemperature < minTemp) {
                minTemp = minTemperature;
            }
        }
        double temperatureDifference = maxTemp - minTemp;

        context.write(key, new DoubleWritable(temperatureDifference));
    }
}
