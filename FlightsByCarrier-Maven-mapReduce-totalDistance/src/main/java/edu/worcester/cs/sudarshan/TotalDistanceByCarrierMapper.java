package edu.worcester.cs.sudarshan;

import java.io.IOException;
import com.opencsv.CSVParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalDistanceByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
                                                      //Input-key, input-value, output-key, output-value
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	if (key.get() > 0) {

    	    String[] lines = new CSVParser().parseLine(value.toString());
          int distance;

          String carrier = lines[8];
          try {
            distance = Integer.parseInt(lines[18].trim());
          } catch (NumberFormatException e) {
            distance = 0;
          }

    	    context.write(new Text(carrier), new IntWritable(distance));
    	}
    }
}
