package com.dataflair.mrStructure;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {
        /** I want to add ALL numbers at each byte!*/
        int sum = 0;
        String[] numbersArray = value.toString().split(" ");
        for (String eachNumber: numbersArray){
            sum += Integer.valueOf(eachNumber);
        }
        context.write(new Text("Sum"), new IntWritable(sum));
    }

}
