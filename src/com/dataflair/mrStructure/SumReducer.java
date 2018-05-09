package com.dataflair.mrStructure;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,Context context){
        //Getting a collection of values
        // adding them up as well
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        try {
            context.write(key, new IntWritable(sum));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
