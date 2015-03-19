package com.MRdesignpatterns.filtering.TopK;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author panzhichun
 *
 */
public class TopKDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		if (args.length != 3) {
		System.err.println("Usage:  <in> <out> <K>");
		System.exit(2);
		}
		Job job =  Job.getInstance(conf, "TopKDesignPatterns");
		job.setJarByClass(TopKDriver.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().set("K", args[2]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
