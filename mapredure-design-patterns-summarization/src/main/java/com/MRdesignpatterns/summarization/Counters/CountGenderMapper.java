package com.MRdesignpatterns.summarization.Counters;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 利用计数器实现不产生任何输出文件的统计功能
 * @author panzhichun 2015-03-18
 * 
 */
public class CountGenderMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

	//统计性别的计数器组
	public static final String STATE_COUNTER_GROUP = "Gender";
	//统计异常数据数量
	public static final String UNKNOWN_COUNTER = "Unknown";
	//统计空数据数量
	public static final String NULL_OR_EMPTY_COUNTER = "NullOrEmpty";
	
	private HashSet<String> Gender = new HashSet<String>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//性别男
		Gender.add("M");
		
		//性别女
		Gender.add("W");
	}
	
	
	@Override
	public void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		
		String inValue = value.toString();
		
		if(StringUtils.isEmpty(inValue.toString())){
			//如果数据为空，则相应的计数器加1
			context.getCounter(STATE_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
		}else{
			boolean unKnown = true;
			for(String gender:Gender){
				if(gender.equals(inValue)){
					context.getCounter(STATE_COUNTER_GROUP, gender).increment(1);
					//成功识别性别，并且退出遍历
					unKnown = false;
					break;
				}
			}
			if(unKnown){
				//异常数据加1
				context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
			}
		}
		
	}




	
}
