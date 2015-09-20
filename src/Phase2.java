import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Phase2 {
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		private Text value1 = new Text();
		private Text key1 = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] vals = line.split("\t");
			
			key1.set(vals[0]);
			value1.set(vals[1]);
			context.write(key1, value1);
		}
	}
	
	
	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			double noOfMonths = 0.0;
			double sum = 0.0;
			
			List<Double> list = new LinkedList<Double>();
			
			for( Text val:values ){
				sum += Double.parseDouble(val.toString());
				list.add(Double.parseDouble(val.toString()));
				noOfMonths++;
			}
			
			double xBar = sum / noOfMonths;
			
			double calc = 0.0;
			
			Iterator<Double> it = list.iterator();
			while(it.hasNext()){
				calc = calc + Math.pow((it.next() - xBar), 2);
			}
			
			double volatility = 0.0;
			if( noOfMonths > 1 && (volatility = Math.sqrt(calc/(noOfMonths - 1))) > 0.0){
					Text text = new Text();
					//text.set("xBar:"+xBar+",noOfMonths:"+noOfMonths+",calc:"+calc+",volatility:"+volatility);
					text.set(volatility+"");
					context.write(key, text);
			}
		}
	}
}
