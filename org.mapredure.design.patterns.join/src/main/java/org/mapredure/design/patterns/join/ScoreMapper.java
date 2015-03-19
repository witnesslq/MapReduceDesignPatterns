package org.mapredure.design.patterns.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入格式是学生ID<tab>分数
 * @author panzhichun
 *
 */
public class ScoreMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	private Text outvalue = new Text();
	
	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		/**
		 * 加上一个标识符，直接输出
		 */
		outvalue.set("S"+value.toString());
		context.write(key, outvalue);
		
		
	}
}
