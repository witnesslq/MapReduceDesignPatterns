package com.MRdesignpatterns.filtering.TopK;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * redure汇总mapper输出的值，并且取出前K个。这个找TopK的方法适用于K值不大的情况下！
 * @author panzhichun
 *
 */
public class TopKReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	
	/**
	 * 记录前K个值
	 */
	private TreeSet<Integer> TopKSet = new TreeSet<Integer>();

	/**
	 * 输出值
	 */
	private IntWritable outValue = new IntWritable();

	/**
	 * 自定义K的值
	 */
	private int K;
	
	/**
	 * 因为mapper输出的K为NullWritable，所以reduce只有一行输入
	 */
	 @Override
	protected void reduce(NullWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		
		for(IntWritable value:values){
			TopKSet.add(value.get());
		}
		/**
		 * TreeSet默认按升序排序，当TreeSet数据量大于K个时，移除第一个数据
		 */
		if (TopKSet.size() > K) {
			TopKSet.pollFirst();
		}
		 
	}
	 
	 
	 
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
	   K = 	Integer.parseInt(context.getConfiguration().get("K"));
	}




	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Iterator<Integer> it = TopKSet.iterator();
		/**
		 * 输出此mapper中的前K个最大的值
		 */
		for (; it.hasNext();) {
			outValue.set(it.next());
			context.write(NullWritable.get(), outValue);
		}
	}
}
