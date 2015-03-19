package com.MRdesignpatterns.hierarchical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HierarchicalDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HierarchicalDesignPatterns");
		job.setJarByClass(HierarchicalDriver.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, PersonMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ScoreMapper.class);
		job.setReducerClass(HierarchicalReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

}
