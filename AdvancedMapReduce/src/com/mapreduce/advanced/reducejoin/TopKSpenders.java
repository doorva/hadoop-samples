package com.mapreduce.advanced.reducejoin;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKSpenders {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJarByClass(TopKSpenders.class);
		job.setNumReduceTasks(1);
		
		// the input to this program is the output of the reducer of ReduceSideJoin
		// that aggregates the total count of transactions and the total amount spent
		// by each customer.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setReducerClass(TopKSpendersReducer.class);
		job.setMapperClass(TopKSpendersMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);
		 
		System.exit(job.waitForCompletion(true)?0:1);

	}
	
	static class TopKSpendersMapper extends Mapper<LongWritable, Text, Text, Text>{
		TreeMap<Double, String> topKSpenders = new TreeMap<Double,String>();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] fields = value.toString().split("\t");
			Double totalSpend = Double.parseDouble(fields[2]);
			String customerName = fields[0];			
			topKSpenders.put(totalSpend, customerName);
			if (topKSpenders.size()>10){
				topKSpenders.remove(topKSpenders.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for(Double amount: topKSpenders.keySet()){
				context.write(new Text(String.valueOf(amount)), new Text(topKSpenders.get(amount)));
			}
		}
				
	}
	
	static class TopKSpendersReducer extends Reducer<Text,Text,Text,DoubleWritable>{
		TreeMap<Double,String> topKSpenders = new TreeMap<Double,String>();
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			for(Double key: topKSpenders.descendingKeySet()){
				context.write(new Text(topKSpenders.get(key)), new DoubleWritable(key));
			}
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(key, values, context);
			for(Text amount: values){
				topKSpenders.put(Double.parseDouble(amount.toString()),key.toString());
				if (topKSpenders.size()>10){
					topKSpenders.remove(topKSpenders.firstKey());
				}
			}
		}
		
	}
}
