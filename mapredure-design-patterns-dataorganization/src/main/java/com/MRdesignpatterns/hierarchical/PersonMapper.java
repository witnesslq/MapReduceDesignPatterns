package com.MRdesignpatterns.hierarchical;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 分层结构模式和连接模式有一点点像，只不过分层结构模式产生的输出是一个层次结构的结果，例如，一个学生和学生对应的各门课程名称
 * 
 * 这个Mapper输入的是一个学生数据集，分别是学生的ID，和学生的资料
 * @author panzhichun
 *
 */
public class PersonMapper extends Mapper<IntWritable,Text, IntWritable, Text> {

	private Text outValue = new Text();
	
	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//直接把学生的ID和对应的个人资料输出，但是个人资料需要加上标识符。因为在reduce阶段会和另外一个数据集的数据组合在一个输入值数组里面
		outValue.set("P"+value.toString());
		
		context.write(key,outValue);
	}
}
