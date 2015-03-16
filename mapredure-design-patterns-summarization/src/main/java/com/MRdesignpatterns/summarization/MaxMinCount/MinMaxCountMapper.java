package com.MRdesignpatterns.summarization.MaxMinCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.MRdesignpatterns.summarization.MaxMinCount.MinMaxCountTuple;


/**
 * 最小最大值和计数值的mapper
 * @author panzhichun 2015-03-16
 *
 */
public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	private final static SimpleDateFormat frmt =new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		/**
		 * 把输入的数据进行一个简单的清洗和提取
		 */
		Map<String,String> formatData = parseData(value.toString());
	    
		/**
		 * 获取创建时间
		 */
		Date outDate = new Date();
		try {
			outDate = frmt.parse(formatData.get("CreationDate"));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		String outUser = formatData.get("UserId");
		
		/**
		 * 把提取到的数据赋值
		 */
		outTuple.setMin(outDate);
		outTuple.setMax(outDate);
		outTuple.setCount(1);
		outUserId.set(outUser);
		
		context.write(outUserId, outTuple);
	}
	
	private Map<String,String> parseData(String value){
		return null;
	}
}
