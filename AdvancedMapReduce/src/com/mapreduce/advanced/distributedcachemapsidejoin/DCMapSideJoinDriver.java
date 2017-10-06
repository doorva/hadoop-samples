package com.mapreduce.advanced.distributedcachemapsidejoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class DCMapSideJoinDriver {
	private static final Logger LOG = Logger.getLogger(DCMapSideJoinDriver.class);

	class DCMapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> abcMap = new HashMap<String, String>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			String[] arr = str.split("\t");
			String state = abcMap.get(arr[0]);
			context.write(new Text(state), value);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			URI[] uris = context.getCacheFiles();
			LOG.info("The path of the URI is : " + uris[0].getPath());
			String fileName = uris[0].toString().replace(uris[0].getPath(), "").replace("#", "./");
			LOG.info("File name is : " + fileName);
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine();
			while (line != null) {
				String[] strarr = line.split("\t");
				String ab = strarr[0];
				String state = strarr[1];
				abcMap.put(ab, state);
				line = reader.readLine();
			}
			reader.close();//close the reader to stop streaming and avoid memory leaks.

		}

	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Job job = Job.getInstance();
		job.setJarByClass(DCMapSideJoinDriver.class);
		job.setMapperClass(DCMapSideJoinMapper.class);
		job.setNumReduceTasks(0);

		if (args.length == 4) {

			String fullCachedFileName = args[0] + "#" + args[1];

			LOG.info("The name for the cached file: " + args[0]);
			LOG.info("The name of the local file: " + args[1]);
			LOG.info("The URL for the cached local file: " + fullCachedFileName);
			LOG.info("The name of the join side file is: " + args[2]);
			LOG.info("The name of the output folder is: " + args[3]);

			// add the file to load into a distributed cache.
			job.addCacheFile(new URI(fullCachedFileName));

			// add the input file
			FileInputFormat.addInputPath(job, new Path(args[2]));

			// add the output path
			FileOutputFormat.setOutputPath(job, new Path(args[3]));

			// set the output key and value types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// delete the output path if it already exists
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(args[3]), true);

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} else {
			LOG.error("Incomplete list of parameters for the program");
			System.exit(-1);
		}
	}

}
