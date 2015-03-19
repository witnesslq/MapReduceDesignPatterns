package com.MRdesignpatterns.partition;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 把输入的数据按照特定的规则进行分组
 * 这个例子中就把学生按照年级进行分组，每个年纪作为一个输出文件,这个功能的一个特点是要先确定分区的数目。这里用1,2,3,4作为年级
 * @author panzhichun
 *
 */
public class PartitionMapper extends Mapper<Object, Text, IntWritable, Text> {

	private HashMap<String, Integer> inValue = new HashMap<String, Integer>();
	private IntWritable outKey = new IntWritable();
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {

		inValue = praseData(value.toString());
		if(inValue.size()>0){
			outKey.set(inValue.get("grade"));
			context.write(outKey, value);
		}
		
	}
	
	private HashMap<String, Integer> praseData(String value){
		HashMap<String, Integer> data = new HashMap<String, Integer>();
		String[] StuInfo = value.split(":");
		if(StuInfo!=null){
			data.put("grade", Integer.parseInt(StuInfo[0]));
		}
	
		return data;
	}
}
