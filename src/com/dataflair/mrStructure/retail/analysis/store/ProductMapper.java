package com.dataflair.mrStructure.retail.analysis.store;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<Object, Text, Text, DoubleWritable>  {

    /**
     * Calculate sales  breakdown  by  product  category  across  all  of  the stores.**/
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString(); // split to string
        // "2012-01-01      09:00   San Jose        Men's Clothing  214.05  Amex"

        final String[] data = line.trim().split("\t"); //split by tab
        //This will result in being split into array of string;
        //Get the product!
        if (data.length == 6){
            //This is the key
            String product = data[3];
            Double salesPrice = Double.valueOf(data[4]);
            context.write(new Text(product), new DoubleWritable(salesPrice));
        }
    }
}
