package com.abdul.hadoop.mapreduce.pattern.dictionary;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
 
public class DictionaryJob {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if (args.length == 0) {
            args = new String[2];
            args[0] = "src/main/resources/customin";
            args[1] = "src/main/resources/customout";
            FileUtils.deleteDirectory(new File(args[1]));
        }
		
		
		try {
			String regex = "^[A-Z]+";
			Configuration conf = new Configuration(true);
			conf.set("record.delimiter.regex", regex);
			Job job=new Job(conf);
			job.setJarByClass(DictionaryJob .class);
			job.setJobName("DictionaryJob ");

			//For this scenarioi we ndon’t need any reducer method so making the number of reducer to zero
			job.setNumReduceTasks(0);
			job.setMapperClass(DictionaryMap.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(CustomInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			try {
				System.exit(job.waitForCompletion(true)?0:1);
			} catch (ClassNotFoundException | InterruptedException e) {
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
