import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class XMLMessageRecordReader extends RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory
			.getLog(ArMLMessageRecordReader.class);

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength=Integer.MAX_VALUE;
	private LongWritable key = new LongWritable();
	private final static Text EOL = new Text("\n");
    private Pattern messageStartTagPattern;
    private Pattern messageEndTagPattern;
    private String messageStartTag ;
    private String messageEndTag ;
    private Text value = new Text();
	private Text valueChunk = new Text();
	private Text ignoreLines = new Text();
    
    
	@Override
	public void close() throws IOException {
		
		if (in != null) in.close();
		valueChunk.clear();
		ignoreLines.clear();
		valueChunk = null;
		ignoreLines =null;
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
		if (start == end)  return 0.0f;
        return Math.min(1.0f, (pos - start) / (float) (end - start));
     }

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		//configuration 
		Configuration job = context.getConfiguration();
		this.messageStartTag = job.get("message.start.tag");
		this.messageEndTag = job.get("message.end.tag");
		
        messageStartTagPattern = Pattern.compile(messageStartTag);
        messageEndTagPattern = Pattern.compile(messageEndTag); 
        
		// get default split of MR
		FileSplit split = (FileSplit) genericSplit;
		
		start = split.getStart();
		end = start + split.getLength();

		// Retrieve file content containing each split
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		// If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }
 
        in = new LineReader(fileIn, job);
 
        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE, 
                            end - start));
        }
 
		// Position is the actual start
		this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		 key.set(pos);
		 
	        int newSize = 0;
	 
	        // Make sure we get at least one record that starts in this Split
	        while (pos < end) {
	 
	            // Read first line and store its content to "value"
	            newSize = readNext(value, maxLineLength,
	                    Math.max((int) Math.min(
	                            Integer.MAX_VALUE, end - pos),
	                            maxLineLength));
	 
	            // No byte to read, seems that we reached end of Split
	            // Break and return false (no key / value)
	            if (newSize == 0) break;
	            	 
	            // Line is read, new position is set
	            pos += newSize;
	 
	            // Line is lower than Maximum record line size
	            // break and return true (found key / value)
	            if (newSize < maxLineLength) break;
	            
	 
	            // Line is too long
	            // Try again with position = position + line offset,
	            // i.e. ignore line and go to next one
	            // TODO: Shouldn't it be LOG.error instead ??
	            LOG.info("Skipped line of size " + 
	                    newSize + " at pos "
	                    + (pos - newSize));
	        }
	 
	         
	        if (newSize == 0) {
	            // We've reached end of Split
	            key = null;
	            value = null;
	            return false;
	        } else {
	            // Tell Hadoop a new line has been found
	            // key / value will be retrieved by
	            // getCurrentKey getCurrentValue methods
	            return true;
	        }
	}

	private int readNext(Text value,int maxLineLength, int maxBytesToConsume)
			throws IOException {

		int offset = 0;
		value.clear();
		boolean skipLine =true;

		for (int i = 0; i < maxBytesToConsume; i++) {

			int offsetTmp = in.readLine(valueChunk, maxLineLength, maxBytesToConsume);
			offset += offsetTmp;
			
			Matcher startTagMatch = messageStartTagPattern.matcher(valueChunk.toString());
			Matcher endTagMatch = messageEndTagPattern.matcher(valueChunk.toString());
								
			if (offsetTmp == 0) break;
			if (endTagMatch.matches()) 
				value.append(valueChunk.getBytes(), 0, valueChunk.getLength());
			if( skipLine && !startTagMatch.matches())
				ignoreLines.append(valueChunk.getBytes(), 0, valueChunk.getLength());
			else
			{
				value.append(valueChunk.getBytes(), 0, valueChunk.getLength());
				value.append(EOL.getBytes(), 0, EOL.getLength());
				skipLine =false;
			}
		}
		return offset;
	}

}
