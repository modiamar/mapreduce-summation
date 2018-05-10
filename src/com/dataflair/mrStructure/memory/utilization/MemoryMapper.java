package com.dataflair.mrStructure.memory.utilization;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class MemoryMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("");

    @Override
    public void map(LongWritable key , Text value, Context context) throws IOException, InterruptedException {
        //		hdtr001 240613,20:50 Average:       473633    319179     "40.26"     77812     63504    936325     71.31    208009     63161
        String valueTokens[] = value.toString().split(" ");
        String hostName = valueTokens[0];
        String date = "";

        String timestamp = "";
        for (int cnt = 1; cnt < valueTokens.length; cnt++)
        {
            if (valueTokens[cnt].length() > 0)
            {
                timestamp = valueTokens[cnt];
                break;
            }
        }
        try
        {
            date = timestamp.split(",")[0];
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        String memoryUsed = "";
        int counter = 0;
        for (int cnt = 0; cnt < valueTokens.length; cnt++)
        {
            if (valueTokens[cnt].length() > 0)
            {
                counter++;
                if (counter == 6)
                {
                    memoryUsed = valueTokens[cnt];
                }
            }
        }
        context.write(new Text(date), new FloatWritable(Float.parseFloat(memoryUsed)));
    }
}
