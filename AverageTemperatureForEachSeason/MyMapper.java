package com.mycompany.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Haebin Noh
// The average temperature for each season for each city

public class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");

        if (!row[5].isEmpty() && !row[6].isEmpty()) {
            context.write(new Text(row[1] + "," + row[3]), new Text(row[5] + "," + row[6]));
        }
    }
    
}
