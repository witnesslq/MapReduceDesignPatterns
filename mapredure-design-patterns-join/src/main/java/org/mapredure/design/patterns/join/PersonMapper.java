package org.mapredure.design.patterns.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入格式就是一个学生ID<tab>用户名称
 * @author panzhichun
 *
 */
public class PersonMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	private Text outvalue = new Text();
	
	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		/**
		 * 加上一个标识符，直接输出
		 */
		outvalue.set("P"+value.toString());
		context.write(key, outvalue);
	}
}
