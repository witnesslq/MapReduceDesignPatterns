package com.MRdesignpatterns.summarization.MedianStdDev;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 这个combiner是关键
 * @author panzhichun
 *
 */
public class MedianStdDevCombiner extends Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {

	protected void reduce(Text key, Iterable<SortedMapWritable> values,
			Context context) throws IOException, InterruptedException {
		
		SortedMapWritable outValue = new SortedMapWritable();
		for (SortedMapWritable v : values) {
			for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
				LongWritable count = (LongWritable) outValue.get(entry.getKey());
				if (count != null) {
					count.set(count.get()+ ((LongWritable) entry.getValue()).get());
					outValue.put(entry.getKey(),count);
				} else {
					outValue.put(entry.getKey(),(entry.getValue()));
				}
			}
		}
		context.write(key, outValue);
	}
}
