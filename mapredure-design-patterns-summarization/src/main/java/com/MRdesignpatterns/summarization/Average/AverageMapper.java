package com.MRdesignpatterns.summarization.Average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.MRdesignpatterns.summarization.Average.CountAverageTuple;

/**
 * 求平均值的mapper
 * @author panzhichun 2015-03-06
 * 假设输入的值没有任何格式问题，每一行分别是用户ID，用户分数
 *
 */
public class AverageMapper extends Mapper<IntWritable, IntWritable, IntWritable, CountAverageTuple> {

	private CountAverageTuple outTuple = new CountAverageTuple();
	
	@Override
	protected void map(IntWritable key, IntWritable value,Context context)
			throws IOException, InterruptedException {
		
		outTuple.setAverage(value.get());
		outTuple.setCount(1);
		context.write(key,outTuple);
		
	}
}
