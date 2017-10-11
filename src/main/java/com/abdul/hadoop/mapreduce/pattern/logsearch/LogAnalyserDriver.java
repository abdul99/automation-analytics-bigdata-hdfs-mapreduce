package com.abdul.hadoop.mapreduce.pattern.logsearch;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

public class LogAnalyserDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 String regex = "^[A-Za-z]{3},\\s\\d{2}\\s[A-Za-z]{3}.*";
		 //   conf.set("textinputformat.record.delimiter", regex); //record.delimiter.regex"
		    conf.set("record.delimiter.regex", regex); //record.delimiter.regex"
		    conf.set("searchQuery","pleff1");
	      Job job = Job.getInstance(conf, "customformat");
	  	job.setInputFormatClass(PatternInputFormat.class);
		job.setJarByClass(LogAnalyserDriver.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	
	 public static void main(String[] args) throws Exception {
	        if (args.length == 0) {
	            args = new String[2];
	            args[0] = "src/main/resources/pattern-in";
	            args[1] = "src/main/resources/patternout";
	            FileUtils.deleteDirectory(new File(args[1]));
	        }

	        int res = ToolRunner.run(new Configuration(), new LogAnalyserDriver(), args);
	        System.exit(res);
	    }
}



class PatternInputFormat
extends FileInputFormat<LongWritable,Text>{

@Override
public RecordReader<LongWritable, Text> createRecordReader(
    InputSplit split,
    TaskAttemptContext context)
                   throws IOException,
              InterruptedException {

return new PatternRecordReader();
}

}


class Map extends Mapper<LongWritable,Text,Text,NullWritable>{
//	Text t = new Text("sum");
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		//String line = value.toString();
		context.write(new Text(key + "---" + value),NullWritable.get());
	}
}	

