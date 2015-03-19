package com.MRdesignpatterns.partition;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author panzhichun
 *
 */
public class PartitionReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	
	@Override
	protected void reduce(IntWritable in, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
			for(Text t:values){
				context.write(in, t);
			}
		
	}
}
