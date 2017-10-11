package com.abdul.hadoop.mapreduce.pattern.dictionary;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//#3 Create the Map class

public class DictionaryMap extends Mapper <LongWritable,Text,Text,Text> {
	protected void map(LongWritable key,Text value,Context context){

		String [] keyValue=value.toString().split("$");

		try {
			context.write(new Text(keyValue[0]),new Text());
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
