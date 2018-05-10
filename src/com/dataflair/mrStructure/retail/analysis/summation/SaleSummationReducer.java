package com.dataflair.mrStructure.retail.analysis.summation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleSummationReducer extends Reducer<Text, DoubleWritable, IntWritable, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0.0;
        int count = 0;
        for (DoubleWritable value: values){
            sum += value.get();
            count++;
        }
        context.write(new IntWritable(count), new DoubleWritable(sum));
    }
}
