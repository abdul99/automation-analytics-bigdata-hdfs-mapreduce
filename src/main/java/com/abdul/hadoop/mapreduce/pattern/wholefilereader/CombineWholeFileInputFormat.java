package com.abdul.hadoop.mapreduce.pattern.wholefilereader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CombineWholeFileInputFormat
        extends CombineFileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException {
        
        return new CombineFileRecordReader((CombineFileSplit) split,
                context, (Class) CombineWholeFileRecordReader.class);
    }

    public static class CombineWholeFileRecordReader
            extends RecordReader<Text, Text> {

        private WholeFileInputFormat inputFormat = new WholeFileInputFormat();
        private final RecordReader<Text, Text> recordReader;

        public CombineWholeFileRecordReader(CombineFileSplit split,
                TaskAttemptContext context, Integer index)
                throws IOException, InterruptedException {
            
            FileSplit filesplit = new FileSplit(split.getPath(index),
                    split.getOffset(index), split.getLength(index),
                    split.getLocations());
            recordReader = inputFormat.createRecordReader(filesplit, context);
        }

        @Override
        public void close() throws IOException {
            recordReader.close();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue()
                throws IOException, InterruptedException {
            return recordReader.nextKeyValue();
        }

        @Override
        public Text getCurrentKey()
                throws IOException, InterruptedException {
            return recordReader.getCurrentKey();
        }

        @Override
        public Text getCurrentValue()
                throws IOException, InterruptedException {
            return recordReader.getCurrentValue();
        }

        @Override
        public float getProgress()
                throws IOException, InterruptedException {
            return recordReader.getProgress();
        }
    }
}