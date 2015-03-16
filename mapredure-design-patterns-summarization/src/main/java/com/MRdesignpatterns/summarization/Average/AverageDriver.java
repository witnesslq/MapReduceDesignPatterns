package com.MRdesignpatterns.summarization.Average;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.MRdesignpatterns.summarization.Average.CountAverageTuple;
import com.MRdesignpatterns.summarization.Average.AverageMapper;
import com.MRdesignpatterns.summarization.Average.AverageReducer;

public class AverageDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
		System.err.println("Usage:  <in> <out>");
		System.exit(2);
		}
		Job job = new Job(conf, "AverageDesignPatterns");
		job.setJarByClass(AverageDriver.class);
		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CountAverageTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
