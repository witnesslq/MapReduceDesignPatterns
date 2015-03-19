package com.MRdesignpatterns.summarization.MedianStdDev;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 传统的中位数和标准差需要排序输出每一个值< 1, 1, 1, 1, 2, 2, 3,4, 5, 5, 5 >,
 * 优化过的形式是这样的(1→4, 2→2, 3→1, 4→1, 5→3)，这个办法极大的减少了内存的使用
 * @author panzhichun 2015-03-16
 * 假设输入数据格式是：时间（小时）<tab> 上网流量（M）,计算一个城市一天每个个小时产生的上网流量的中位数和标准差
 */
public class MedianStandardMapper extends Mapper<Text, Text, Text, SortedMapWritable> {

	
	SortedMapWritable outValue = new SortedMapWritable();
	LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(Text key,Text value,Context context)
			throws IOException, InterruptedException {
	
		
		
		outValue.put(new LongWritable(Long.parseLong(value.toString())), one);
		
		/**
		 * 把输入的数据转换成时间，流量和计数输出
		 */
		context.write(key, outValue);
	}
}
