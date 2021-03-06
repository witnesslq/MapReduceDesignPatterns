package com.MRdesignpatterns.summarization.Average;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.MRdesignpatterns.summarization.Average.CountAverageTuple;

/**
 * 求平均值的redure
 * @author panzhichun 2015-03-16
 * 非常简单，但是大大减低了中间记录数，优化了shuffle的过程
 */
public class AverageReducer extends Reducer<IntWritable, CountAverageTuple,IntWritable, CountAverageTuple> {

	private  CountAverageTuple outTuple = new CountAverageTuple();
	
	@Override
	protected void reduce(IntWritable key,Iterable<CountAverageTuple> values,Context context)
			throws IOException, InterruptedException {
	
		/**
		 * 记录每个用户的总分
		 */
		long sum = 0;
		
		/**
		 * 记录每个用户的分数记录数
		 */
		long count = 0;
		
		/**
		 * 使用一个迭代器，性能比for循环要好一些
		 */
		Iterator<CountAverageTuple> it = values.iterator();
		while(it.hasNext()){
			CountAverageTuple catp = new CountAverageTuple();
			catp = it.next();
			count = catp.getCount();
			
			sum += count*catp.getAverage();
		}
		outTuple.setCount(count);
		outTuple.setAverage(sum/count);
		
		context.write(key, outTuple);
	}

	
}
