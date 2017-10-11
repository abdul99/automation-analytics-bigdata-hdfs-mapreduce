package com.abdul.hadoop.mapreduce.pattern.delimiter;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
public class SearchKeywordInputFormat extends FileInputFormat<LongWritable,Text> {

	public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		SearchKeywordRecordReader reader = new SearchKeywordRecordReader();
		reader.initialize(split,context);
		return reader;
	}
	@Override 
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}
}
