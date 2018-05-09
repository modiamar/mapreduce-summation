package com.dataflair.mrStructure.retail.analysis.sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StoreReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /**
         * Sum them up
         */
        Double sum = 0.0;
        for(Text value : values){
            if(!key.toString().equals("bad-record-key")) {
                Double doubleValue = Double.valueOf(value.toString());
                sum += doubleValue;
            }
        }
        context.write(key, new DoubleWritable(sum));
    }
}
