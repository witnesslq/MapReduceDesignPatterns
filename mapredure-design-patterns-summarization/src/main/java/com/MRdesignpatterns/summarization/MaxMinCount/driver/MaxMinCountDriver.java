package com.MRdesignpatterns.summarization.MaxMinCount.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.MRdesignpatterns.summarization.MaxMinCount.beans.MinMaxCountTuple;
import com.MRdesignpatterns.summarization.MaxMinCount.mapper.MinMaxCountMapper;
import com.MRdesignpatterns.summarization.MaxMinCount.redure.MinMaxCountReducer;

public class MaxMinCountDriver {

	public static void main(String[] args) throws Exception {
		
        Configuration conf = new Configuration();
		
		if (args.length != 2) {
		System.err.println("Usage:  <in> <out>");
		System.exit(2);
		}
		Job job = new Job(conf, "NumericalDesignPatterns");
		job.setJarByClass(MaxMinCountDriver.class);
		job.setMapperClass(MinMaxCountMapper.class);
		job.setCombinerClass(MinMaxCountReducer.class);
		job.setReducerClass(MinMaxCountReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}