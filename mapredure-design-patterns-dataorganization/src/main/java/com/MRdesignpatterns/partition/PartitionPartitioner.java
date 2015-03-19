package com.MRdesignpatterns.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 直接用输入的年级作为分区的位置
 * @author panzhichun
 *
 */
public class PartitionPartitioner extends Partitioner<Integer, Text> {

	@Override
	public int getPartition(Integer key, Text value, int numPartitions) {
		
		return key;
	}

}
