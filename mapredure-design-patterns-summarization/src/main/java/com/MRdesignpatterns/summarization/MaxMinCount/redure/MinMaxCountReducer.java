package com.MRdesignpatterns.summarization.MaxMinCount.redure;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.MRdesignpatterns.summarization.MaxMinCount.beans.MinMaxCountTuple;

/**
 * 最小，最大，计数 
 * @author panzhichun 2015-03-16
 *
 */
public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	@Override
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values,Context context)
			throws IOException, InterruptedException {
		
        /**
        * 每一次调用都要先初始化一次输出值
        */
		outTuple.setCount(0);
		outTuple.setMax(null);
		outTuple.setMin(null);
		
		int count =0;
		
		/**
		 * 使用一个迭代器，性能比for循环要好一些
		 */
		Iterator<MinMaxCountTuple> it = values.iterator();
		if(it.hasNext()){
			MinMaxCountTuple mmcp = new MinMaxCountTuple();
			mmcp =  it.next();
			
			/**
			 * 如果outTuple(输出元组)最小值为空，则直接赋值，否则和新输入值比较，取最小者
			 */
			if(outTuple.getMin()==null||mmcp.getMin().compareTo(outTuple.getMin())<0){
				outTuple.setMin(mmcp.getMin());
			}
			
			/**
			 * 如果outTuple(输出元组)最大值为空，则直接赋值，否则和新输入值比较，取最大者
			 */
			if(outTuple.getMax()==null||mmcp.getMax().compareTo(outTuple.getMax())<0){
				outTuple.setMax(mmcp.getMax());
			}
			
			/**
			 * 计数值累加，计算用户的总记录数
			 */
			count += mmcp.getCount();
			
			
			
		}
		
		/**
		 * 输出一个key对应的记录
		 */
		outTuple.setCount(count);
		context.write(key, outTuple);
		
	}
}
