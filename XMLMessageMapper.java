
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArMLMessageMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
 
    private Text out = new Text();
 
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
 
        out.set(key.toString() + value);
        context.write(out, NullWritable.get());
    }
}
