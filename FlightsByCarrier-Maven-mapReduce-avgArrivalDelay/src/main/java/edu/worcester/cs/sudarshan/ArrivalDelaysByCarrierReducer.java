// Sudarshan
//Question: What is the average arrival delay time of each carrier?

package edu.worcester.cs.sudarshan;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ArrivalDelaysByCarrierReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key,
                          Iterable<DoubleWritable> values,
                          Context context) throws IOException, InterruptedException {

      Double sum = 0.0;
      Integer count = 0;

    	for (DoubleWritable value:values) {
    	    sum+= value.get();
          count++;
    	}

      Double avg = sum/count;
    	context.write(key, new DoubleWritable(avg));

    }
}
