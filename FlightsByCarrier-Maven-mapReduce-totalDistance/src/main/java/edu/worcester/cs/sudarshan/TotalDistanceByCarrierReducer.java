//sudarshan
//What is the total distance travelled by each carrier?

package edu.worcester.cs.sudarshan;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalDistanceByCarrierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key,
                          Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

      int sum = 0;
    	for (IntWritable value:values) {
    	    sum+= value.get();
    	}
    	context.write(key, new IntWritable(sum));
    }
}
