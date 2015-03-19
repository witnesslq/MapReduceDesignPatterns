package com.MRdesignpatterns.summarization.Counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.MRdesignpatterns.summarization.Average.CountAverageTuple;

/**
 * 只需要在任务结束后，获取计数器的信息就可以知道统计的男女性别数量了。同时需要把输出目录删除
 * 
 * @author panzhichun
 * 
 */
public class CountGenderDriver {

	
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
		
		Job job =  Job.getInstance(conf, "CountDesignPatterns");
		job.setJarByClass(CountGenderDriver.class);
		job.setMapperClass(CountGenderMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int code = job.waitForCompletion(true) ? 0 : 1;
		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(
					CountGenderMapper.STATE_COUNTER_GROUP)) {
				System.out.println(counter.getDisplayName() + "\t"
						+ counter.getValue());
			}
		}
		//再次获取FS
		outfs =outputPath.getFileSystem(conf);
		//删除输出文件
		if(outfs.exists(outputPath)){
    		try{
    			outfs.delete(outputPath,true);
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    		outfs.close();
    	}
	}
}
