package com.abdul.hadoop.mapreduce.pattern.delimiter;

 
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SearchKeyJob extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "serachjob");

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SearchKeywordInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

       return job.waitForCompletion(true) ? 0 : 1;
        
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[2];
            args[0] = "src/main/resources/searchdemo";
            args[1] = "src/main/resources/searchout";
            FileUtils.deleteDirectory(new File(args[1]));
        }

        int res = ToolRunner.run(new Configuration(), new SearchKeyJob(), args);
        System.exit(res);
    }
}