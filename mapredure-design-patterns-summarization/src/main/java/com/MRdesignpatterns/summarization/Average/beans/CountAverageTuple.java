package com.MRdesignpatterns.summarization.Average.beans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * 为了使用Combiner优化，自定义一个计算平均值的输出值，记录下相关的记录数
 * @author panzhichun 2015-06-16
 *
 */
public class CountAverageTuple implements Writable {

	/**
	 * 记录数
	 */
	private long count ;
	
	/**
	 * 平均值
	 */
	private long average;
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public long getAverage() {
		return average;
	}

	public void setAverage(long average) {
		this.average = average;
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readLong();
		average = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(count);
		out.writeLong(average);
	}

}
