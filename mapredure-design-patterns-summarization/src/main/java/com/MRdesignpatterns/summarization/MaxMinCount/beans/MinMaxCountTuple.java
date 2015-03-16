package com.MRdesignpatterns.summarization.MaxMinCount.beans;

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
     * 计算一个key记录的最小日期
     */
	private Date min = new Date();
	
	/**
	 * 计算一个key记录的最大日期
	 */
	private Date max = new Date();
	
	/**
	 * 计算一个key对应的总记录数
	 */
	private long count = 0;
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	public void readFields(DataInput in) throws IOException {
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	public Date getMin() {
		return min;
	}

	public void setMin(Date min) {
		this.min = min;
	}

	public Date getMax() {
		return max;
	}

	public void setMax(Date max) {
		this.max = max;
	}
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
	   return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}

}
