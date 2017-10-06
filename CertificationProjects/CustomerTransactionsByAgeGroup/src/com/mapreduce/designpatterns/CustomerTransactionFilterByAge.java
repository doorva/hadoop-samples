package com.mapreduce.designpatterns;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerTransactionFilterByAge {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance();
		job.setJarByClass(CustomerTransactionFilterByAge.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		job.setMapperClass(CustomerTransactionFilterByAgeMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	static class CustomerTransactionFilterByAgeMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			if (key != null && !key.toString().trim().equals("")) {
				String str = value.toString();
				String[] strarr = str.split("\t");
				int age = Integer.parseInt(strarr[2]);
				if (age >= 20 && age <= 50) {
					context.write(key, new Text(strarr[1]));
				}
			}
		}
	}
}
