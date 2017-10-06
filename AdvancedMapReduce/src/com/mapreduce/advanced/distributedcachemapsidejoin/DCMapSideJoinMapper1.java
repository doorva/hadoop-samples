package com.mapreduce.advanced.distributedcachemapsidejoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DCMapSideJoinMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Map<String, String> abcMap = new HashMap<String, String>();
	private static final Logger LOG = Logger.getLogger(DCMapSideJoinMapper1.class);

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
