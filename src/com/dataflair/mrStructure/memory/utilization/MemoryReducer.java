package com.dataflair.mrStructure.memory.utilization;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MemoryReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (FloatWritable value : values) {
            sum+= value.get();
            count++;
        }
        context.write(key, new FloatWritable(sum/count));
    }

}
