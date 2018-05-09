package com.dataflair.mrStructure;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text output;
    private IntWritable sum;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        /**Given a input Key Value pair, map it to the number of occurences of each word*/
        sum.set(1);
        String[] wordArray = value.toString().split(" ");
        for (int i = 0; i < wordArray.length; i++) {
            context.write(output, sum);
        }
    }
}