package com.MRdesignpatterns.hierarchical;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 这个Mapper处理分数数据集的数据，对应的学生ID为key，分数信息为value
 * @author panzhichun
 *
 */
public class ScoreMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	private Text outValue = new Text();
	
	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//直接把学生的ID和对应的分数信息，但是分数信息需要加上标识符。因为在reduce阶段会和另外一个数据集的数据组合在一个输入值数组里面
		outValue.set("S"+value.toString());
		
		context.write(key,outValue);
		
		
	}
}
