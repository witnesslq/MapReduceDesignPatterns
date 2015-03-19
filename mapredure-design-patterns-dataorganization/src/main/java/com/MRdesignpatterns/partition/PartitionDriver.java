package com.MRdesignpatterns.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class PartitionDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "PartitionDesignPatterns");
		job.setJarByClass(PartitionDriver.class);
		job.setMapperClass(PartitionMapper.class);
		job.setPartitionerClass(PartitionPartitioner.class);
		job.setReducerClass(PartitionReducer.class);
		job.setNumReduceTasks(4);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

}
