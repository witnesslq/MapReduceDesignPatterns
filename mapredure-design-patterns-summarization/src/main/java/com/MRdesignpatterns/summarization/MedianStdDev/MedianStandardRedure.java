package com.MRdesignpatterns.summarization.MedianStdDev;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;


public class MedianStandardRedure extends Reducer<IntWritable, SortedMapWritable, IntWritable, DoubleWritable> {

	private DoubleWritable outTuple = new DoubleWritable();
	
	private TreeMap<Integer,Long> netWorkFlow = new TreeMap<Integer,Long>();
	
	@Override
	protected void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context)
			throws IOException, InterruptedException {
		
		netWorkFlow.clear();
		outTuple.set(0);
		double sumFlow = 0;
		long totalCount = 0;
		
		Iterator<SortedMapWritable> it =  values.iterator();
		if(it.hasNext()){
			Iterator<Entry<WritableComparable, Writable>>  entrySetIterator = it.next().entrySet().iterator();
			if(entrySetIterator.hasNext()){
				Entry<WritableComparable, Writable>  entry =  entrySetIterator.next();
				int flow = ((IntWritable)entry.getKey()).get();
				long count = ((LongWritable)entry.getValue()).get();
				
				//计算该小时的总输入值数目
				totalCount += count;
				
				//计算该小时的总流量
				sumFlow +=count*flow;
				
				Long storedflow = netWorkFlow.get(flow);
				
				/**
				 * 保存每一个流量值和相应的出现次数
				 */
				if(storedflow==null){
					netWorkFlow.put(flow, count);
				}else{
					netWorkFlow.put(flow, storedflow+count);
				}
				
			}
		}
		
		//计算中值的位置索引
		long medianIndex = totalCount/2L;
		//保存遍历过程中上一次的位置索引
		long preIndex = 0;
		//当前位置索引
		long index = 0;
		//上一次遍历的key
		long preKey = 0;
		
		for(Entry<Integer, Long> entry:netWorkFlow.entrySet()){
			index = index+entry.getValue();
			//如果中值位置索引大于前一个位置索引，并且小于当前位置索引，那么中值就在里面
			if(preIndex<=medianIndex&&medianIndex<index){
				//如果中值索引是双数，并且等于上一次遍历的索引，那么中值就是两个位置的值的平均数
				if(totalCount%2==0&&preIndex==medianIndex){
					outTuple.set((double)(entry.getKey()+preKey)/2.0);
				}else{
					outTuple.set(entry.getKey());
				}
			}
		}
		
		context.write(key, outTuple);
		
		
	}

	
	
}
