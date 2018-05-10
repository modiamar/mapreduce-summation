package com.dataflair.mrStructure.retail.analysis.summation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesSummationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        /**
         * Key is the store, each sale is the value
         */
        String store= data[2];
        Double amount = Double.valueOf(data[4]);

        context.write(new Text("ConstantSum"), new DoubleWritable(amount));
    }
}
