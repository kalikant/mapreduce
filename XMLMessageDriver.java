import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XMLMessageDriver extends Configured implements Tool{
	
	public static String messageStartTag = "<XMLMessage>";
	public static String messageendTag = "</XMLMessage>";
	

	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Two parameters are required for XMLMessageDriver - <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJobName("XMLMessageInputFormat Test");
		job.setJarByClass(XMLMessageDriver.class);

		// setting custom properties
		job.getConfiguration().set("message.start.tag", messageStartTag);
		job.getConfiguration().set("message.end.tag", messageendTag);
		
		job.setInputFormatClass(XMLMessageInputFormat.class);
		ArMLMessageInputFormat.addInputPath(job, new Path(args[0]));
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(XMLMessageMapper.class);
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new XMLMessageDriver(), args);
		System.exit(exitCode);
	}
	
}
