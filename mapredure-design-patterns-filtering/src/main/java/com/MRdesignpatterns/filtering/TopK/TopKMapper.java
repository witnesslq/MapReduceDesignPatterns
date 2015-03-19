package com.MRdesignpatterns.filtering.TopK;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 找出数据中某种规则的前K个值，这个例子是找出阿拉伯数字数据集中的最大前10个数
 * 
 * @author panzhichun
 * 
 */
public class TopKMapper extends Mapper<Object, IntWritable, NullWritable, IntWritable> {

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

	@Override
	protected void map(Object key, IntWritable value, Context context)
			throws IOException, InterruptedException {

		TopKSet.add(value.get());
		/**
		 * TreeSet默认按升序排序，当TreeSet数据量大于K个时，移除第一个数据
		 */
		if (TopKSet.size() > K) {
			TopKSet.pollFirst();
		}

	}

	

	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		Iterator<Integer> it = TopKSet.iterator();
		/**
		 * 输出此mapper中的前K个最大的值
		 */
		for (; it.hasNext();) {
			outValue.set(it.next());
			context.write(NullWritable.get(), outValue);
		}

	}



	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	   K = 	Integer.parseInt(context.getConfiguration().get("K"));
	}

}
