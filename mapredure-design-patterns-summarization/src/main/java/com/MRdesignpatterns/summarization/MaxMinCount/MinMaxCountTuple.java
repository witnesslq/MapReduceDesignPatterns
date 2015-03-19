package com.MRdesignpatterns.summarization.MaxMinCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

/**
 * 存储最大值，最小值和计数
 * @author panzhichun 2015-03-15
 *
 */
public class MinMaxCountTuple implements Writable {
    
	
    /**
     * 计算一个
     */
	private long min ;
	
	/**
	 * 计算一个key记录的最大日期
	 */
	private long max ;
	
	/**
	 * 计算一个key对应的总记录数
	 */
	private long count;
	
	
	public void readFields(DataInput in) throws IOException {
		min = in.readLong();
		max = in.readLong();
		count = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(min);
		out.writeLong(max);
		out.writeLong(count);
	}
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
	   return min + "\t" + max + "\t" + count;
	}

	public Long getMin() {
		return min;
	}

	public void setMin(long min) {
		this.min = min;
	}

	public Long getMax() {
		return max;
	}

	public void setMax(long max) {
		this.max = max;
	}

	
	

}
