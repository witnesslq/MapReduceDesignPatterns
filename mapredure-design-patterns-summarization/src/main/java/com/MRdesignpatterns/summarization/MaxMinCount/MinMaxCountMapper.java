package com.MRdesignpatterns.summarization.MaxMinCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 最小最大值和计数值的mapper
 * 找出一个班中总人数最大学号，最小学号
 * @author panzhichun 2015-03-16
 *
 */
public class MinMaxCountMapper extends Mapper<Text, Text, Text, MinMaxCountTuple> {

	private Text outUser = new Text();
	
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	@Override
	protected void map(Text key, Text value,Context context)
			throws IOException, InterruptedException {
		
		/**
		 * 把提取到的数据赋值
		 */
		outTuple.setMin(Integer.parseInt(value.toString()));
		outTuple.setMax(Integer.parseInt(value.toString()));
		outTuple.setCount(1);
		outUser.set(key);
		
		context.write(outUser, outTuple);
	}
	
	
}
