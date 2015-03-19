package org.mapredure.design.patterns.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 3) {
		System.err.println("Usage:  <in> <in> <out>");
		System.exit(2);
		}
		Job job =  Job.getInstance(conf, "JoinDesignPatterns");
		job.setJarByClass(JoinDriver.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class,
				PersonMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class,
		        ScoreMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
