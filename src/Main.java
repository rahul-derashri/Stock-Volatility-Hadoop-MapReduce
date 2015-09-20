import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main {

	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		Job job2 = Job.getInstance();
		Job job3 = Job.getInstance();
		
		job.setJarByClass(Main.class);
		// Mapper
		job.setMapperClass(Phase1.Map1.class);
		// Reducer
		job.setReducerClass(Phase1.Reduce1.class);
		
		job2.setJarByClass(Main.class);
		job2.setMapperClass(Phase2.Map2.class);
		job2.setReducerClass(Phase2.Reduce2.class);
		
		job3.setJarByClass(Main.class);
		job3.setMapperClass(Phase3.Map3.class);
		job3.setReducerClass(Phase3.Reduce3.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		FileInputFormat.setInputDirRecursive(job, true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"_Intermediate_1"));
		
		FileInputFormat.addInputPath(job2, new Path(args[1]+"_Intermediate_1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_Intermediate_2"));
		
		FileInputFormat.addInputPath(job3, new Path(args[1]+"_Intermediate_2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job.setJarByClass(Main.class);
		job2.setJarByClass(Main.class);
		job3.setJarByClass(Main.class);
		
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
		job3.waitForCompletion(true);
		
	}

}
