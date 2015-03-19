package com.MRdesignpatterns.hierarchical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author panzhichun
 *
 */
public class HierarchicalReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	String outUser = new String();
	Text outValue = new Text();
	List<String> outScore = new ArrayList<String>();
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		
		outUser = null;
		outScore.clear();
		
		Iterator<Text> it = values.iterator();
		
		while(it.hasNext()){
			String value = it.next().toString();
			//如果传过来的是学生个人信息，侧标识符是‘p’
			if(value.charAt(0)=='P'){
				outUser = value.substring(1, value.length()).trim();
			}else {
				outScore.add(value.substring(1, value.length()).trim());
			}
			
		}
		
		if(outUser!=null){
			//把两个来自不同数据集的数据组织在一起
			outValue.set(outUser+":"+outScore.toString());
			context.write(key,outValue );
		}
	}
}
