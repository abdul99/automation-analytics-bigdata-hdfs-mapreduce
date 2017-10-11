package com.abdul.hadoop.mapreduce.pattern.dictionary;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.hadoop.util.LineReader;
 

public class CustomeRecordReader extends RecordReader<LongWritable, Text> {

	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private long start, end, pos;
	private FSDataInputStream fsin;
	private DataOutputBuffer buffer = new DataOutputBuffer();
	private int maxLineLength;
	private LineReader lineReader;
	private final static Text keyValueDel = new Text("$");
	private Pattern delimiterPattern;
	private String delimiterRegex;
	private Text matchedText=new Text();
	private boolean firstOccurance=true;

	@Override
	public void initialize(InputSplit inputflSplit, TaskAttemptContext Context)
			throws IOException, InterruptedException {

		FileSplit flSplit = (FileSplit) inputflSplit;
		Configuration conf = Context.getConfiguration();
		this.delimiterRegex = conf.get("record.delimiter.regex");
				this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",
						Integer.MAX_VALUE);
				delimiterPattern = Pattern.compile(delimiterRegex);
				start = flSplit.getStart();
				end = start + flSplit.getLength();
				final Path filpath = flSplit.getPath();
				FileSystem fs = filpath.getFileSystem(conf);
				fsin = fs.open(filpath);
				boolean skipFirstLine = false;
				if (start != 0) {
					skipFirstLine = true;
					// - start;
					fsin.seek(start);
				}
				lineReader = new LineReader(fsin, conf);
				if (skipFirstLine) {
					Text dummy = new Text();
					// Reset "start" to "start + line offset"
					start += lineReader.readLine(dummy, 0,
							(int) Math.min((long) Integer.MAX_VALUE, end - start));
				}
				this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		//In the dictionary text all the word is written in Capital case. I am checking this by regex. If //it matches then it is word and meaning of the word is next all line prior to the next //matched dictionary word . thus forming a new line where word and meaning of the word is //separating by $ which is separating in map method as key and value
		int lineSize = 0;
		while (pos < end) {
			lineSize = readNext(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength)); //it read the text and stored into value and return the size //of the line

			if (lineSize == 0) { // Break and return false if the newly read line size is zero (i.e. //pointer is reached to the end. no line is remain to read)
				break;
			}
			// setting the new position of the pointer
			pos += lineSize;
			// Line is lower than Maximum record line size
			// break and return true (found key / value)
			if (lineSize < maxLineLength) {
				key.set(pos);
				break;
			}
		}
		if (lineSize == 0) {
			// We’ve reached end of input split
			key = null;
			value = null;
			return false;
		} else {
			//return the new line which has been formed by appending multiple line
			return true;
		}
	}

	private int readNext(Text text, int maxLineLength, int maxBytesToConsume)
			throws IOException {

		int offset = 0;
		text.clear();
		Text tempText = new Text();
		if(!firstOccurance){
			text.append(matchedText.getBytes(), 0, matchedText.getLength());
			text.append(keyValueDel.getBytes(), 0, keyValueDel.getLength());
		}
		for (int i = 0; i < maxBytesToConsume; i++) {

			int offsetTmp = lineReader.readLine(tempText, maxLineLength,
					maxBytesToConsume);
			offset += offsetTmp;
			Matcher m = delimiterPattern.matcher(tempText.toString());

			if (offsetTmp == 0) {
				break;
			}
			if (m.matches()) {
				if(firstOccurance){
					text.append(tempText.getBytes(), 0, tempText.getLength());
					text.append(keyValueDel.getBytes(), 0, keyValueDel.getLength());
					firstOccurance=false;
				}else{
					matchedText=tempText;
					break;
				}
			} else {
				text.append(tempText.getBytes(), 0, tempText.getLength());
			}

		}
		return offset;

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

}


