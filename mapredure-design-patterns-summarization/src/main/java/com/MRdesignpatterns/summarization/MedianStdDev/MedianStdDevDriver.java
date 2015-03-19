package com.MRdesignpatterns.summarization.MedianStdDev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianStdDevDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}
		
		Path outputPath = new Path(args[1]);
        FileSystem outfs =outputPath.getFileSystem(conf);
    	//这里是为了调试的时候不必每次都手动删除输出文件
    	if(outfs.exists(outputPath)){
    		try{
    			outfs.delete(outputPath,true);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		outfs.close();
    	}
		
		Job job = Job.getInstance(conf, "MedianStdDevDesignPatterns");
		job.setJarByClass(MedianStdDevDriver.class);
		job.setMapperClass(MedianStandardMapper.class);
		job.setCombinerClass(MedianStdDevCombiner.class);
		job.setReducerClass(MedianStandardReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SortedMapWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
