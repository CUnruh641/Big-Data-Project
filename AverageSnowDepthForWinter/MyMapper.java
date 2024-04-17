package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Haebin Noh
// The average snow depth for winter season for each city

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");

        if (!row[8].isEmpty() && row[3].equalsIgnoreCase("winter")) {
            context.write(new Text(row[1] + "," + row[3]), new DoubleWritable(Double.parseDouble(row[8])));
        }
    }
}
