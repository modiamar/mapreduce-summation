package com.dataflair.mrStructure;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        int incrememtor = 0;
        for (IntWritable value : values) {
            incrememtor += value.get();
        }
        result.set(incrememtor);
        context.write(key, result); //Return a result of Text and IntWritable
    }
  }