package com.MRdesignpatterns.summarization.MedianStdDev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianStdDevDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MedianStdDevDesignPatterns");
		job.setJarByClass(MedianStdDevDriver.class);
		job.setMapperClass(MedianStandardMapper.class);
		job.setCombinerClass(MedianStdDevCombiner.class);
		job.setReducerClass(MedianStandardRedure.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
