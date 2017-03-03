package edu.worcester.cs.sudarshan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalDistanceByCarrier {

    public static void main(String[] args) throws Exception {

      //Define mapreduce job
    	Job job = Job.getInstance();
    	job.setJarByClass(TotalDistanceByCarrier.class);
    	job.setJobName("TotalDistanceByCarrier");

      //Set input and output locations
    	TextInputFormat.addInputPath(job, new Path(args[0]));
      TextOutputFormat.setOutputPath(job, new Path(args[1]));

      //Set input and output formats
    	job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      //Set mapper and reducer classes
    	job.setMapperClass(TotalDistanceByCarrierMapper.class);
    	job.setReducerClass(TotalDistanceByCarrierReducer.class);

      //Output types
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);

    	job.waitForCompletion(true);
    }
}
