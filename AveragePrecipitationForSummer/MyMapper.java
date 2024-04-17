package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Haebin Noh
// The average precipitation for summer season for each city

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");

        if (!row[7].isEmpty() && row[3].equalsIgnoreCase("summer")) {
            context.write(new Text(row[1] + "," + row[3]), new DoubleWritable(Double.parseDouble(row[7])));
        }
    }
    
}
