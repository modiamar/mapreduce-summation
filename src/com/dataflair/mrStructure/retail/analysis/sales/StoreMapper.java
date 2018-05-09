package com.dataflair.mrStructure.retail.analysis.sales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StoreMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Calculate
         sales
         break down  by  store across  all  of the  stores  (handle  bad  records)
         */
        String[] row = value.toString().split("\t");
        if(row.length == 6){
            String newKey = row[2];
            String newValue = row[4];
            context.write(new Text(newKey), new Text(newValue));
        }
        else{
            context.write(new Text("bad-record-key"), value);
        }

    }
}
