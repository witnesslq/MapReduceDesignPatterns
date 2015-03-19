package org.mapredure.design.patterns.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<IntWritable, Text, Text, Text> {

	private List<Text> userList = new ArrayList<Text>();
	private List<Text> scoreList = new ArrayList<Text>();

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		userList.clear();
		userList.clear();

		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			String inValue = it.next().toString();
			if (inValue.charAt(0) == 'P') {
				userList.add(new Text(inValue.substring(1)));
			} else {
				scoreList.add(new Text(inValue.substring(1)));
			}
		}

		try {
			// 执行join
			execJoin(context);
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	private void execJoin(Context context) throws Exception {

		/**
		 * 演示left join
		 */
		for (Text user : userList) {
			// 如果右表是空的，则输出一个空值
			if (!scoreList.isEmpty()) {
				for (Text score : scoreList) {
					context.write(user, score);
				}
			} else {
				context.write(user, new Text("NULL"));
			}
		}
		
		
		/**
		 * 演示right join
		 */
		for (Text score : scoreList) {
			// 如果右表是空的，则输出一个空值
			if (!userList.isEmpty()) {
				for (Text user : userList) {
					context.write(user, score);
				}
			} else {
				context.write(new Text("NULL"),score);
			}
		}

		

		/**
		 * 演示full join
		 */
		if(!userList.isEmpty()){
			for(Text user:userList){
				if(!scoreList.isEmpty()){
					for(Text score:scoreList){
						context.write(user, score);
					}
				}else{
					context.write(user, new Text("NULL"));
				}
			}
		}else{
			for(Text score:scoreList){
				context.write(new Text("NULL"), score);
			}
		}
	}

}
