package edu.worcester.cs.sudarshan;

import java.io.IOException;
import com.opencsv.CSVParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ArrivalDelaysByCarrierMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
                                                      //Input-key, input-value, output-key, output-value
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	if (key.get() > 0) {

    	    String[] lines = new CSVParser().parseLine(value.toString());

          String carrier = lines[8];
          Double arrDelay;

          try {
            arrDelay = Double.parseDouble(lines[14].trim());
          } catch (NumberFormatException e) {
            arrDelay = 0.0;
          }

    	    context.write(new Text(carrier), new DoubleWritable(arrDelay));
    	}
    }
}
