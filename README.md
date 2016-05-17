# ACD_BDD_Session_6_Assignment_1_Main
Find the total number of medals that each country won in swimming.

package Olympic;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
public class OlympicMapper extends Mapper<LongWritable, Text, Text, Text>{
@Override
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
	String values = value.toString();
	String[] records = values.split("\t");
	if(records[5].compareToIgnoreCase("Swimming")==0)
	{
		context.write(new Text(records[2]), new Text(records[9]));
	}
}
}

package Olympic;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OlympicReducer extends Reducer <Text, Text, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
	throws IOException, InterruptedException
	{
		int sum=0;
			    for(Text values : value)
			    {
				String medals = values.toString();
				int total= Integer.parseInt(medals);
				sum=sum+total;
			    }
		context.write(key, new IntWritable(sum));
	}
}

package Olympic;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
public class OlympicDriver extends Configured implements Tool{
	public static void main(String args[]) throws Exception{
		ToolRunner.run(new Configuration(), new OlympicDriver(), args);
	}

	public int run(String args[])throws Exception{
		
		Job job = Job.getInstance();
		job.setJobName("Abhinav Olympic Code");
		job.setJarByClass(OlympicDriver.class);
		
		job.setMapperClass(OlympicMapper.class);
		job.setReducerClass(OlympicReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}
}
