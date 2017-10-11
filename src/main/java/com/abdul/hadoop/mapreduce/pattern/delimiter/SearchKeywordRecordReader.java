package com.abdul.hadoop.mapreduce.pattern.delimiter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
public class SearchKeywordRecordReader extends RecordReader<LongWritable,Text> {
	private Path splitFilePath = null;
	private Text value = new Text();
	private LongWritable key = new LongWritable();
	FSDataInputStream filein = null;
	private boolean stillInChunk = true;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private long start;
	private long end;
	private byte[] endTag = "$$Keyword".getBytes(); 
	
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		FileSplit fileSplit = (FileSplit) split;
		start =  fileSplit.getStart();
		end = start + fileSplit.getLength();
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		splitFilePath = fileSplit.getPath();
		filein = fs.open(splitFilePath);
		filein.seek(start);
		if(start!=0){
			readUntilMatch(endTag, false);
		}
	}

	// function which generate a record with key value pair
	public boolean nextKeyValue() throws IOException{
		if(!stillInChunk) return false;
		boolean status = readUntilMatch(endTag,true);
		value = new Text();
		value.set(buffer.getData(),0,buffer.getLength());
		key = new LongWritable();
		buffer.reset();
		if(!status){
			stillInChunk = false;
		}
		return true;		
	}

	private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i=0;
		while(true){
			int nextByte = filein.read();
			if(nextByte == -1) return false;
			if(withinBlock) buffer.write(nextByte);
			if(nextByte == match[i]){
				i++;
				if(i>=match.length){
					return filein.getPos() < end;
				}
			} 
			else i=0;
		}
	}

	@Override
	public void close() throws IOException {
		filein.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	         return 0;
	}
}
