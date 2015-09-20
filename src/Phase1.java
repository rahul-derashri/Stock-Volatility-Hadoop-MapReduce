import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Phase1 {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		private Text key1 = new Text();
		private Text value1 = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
				String line = value.toString();
				
				if( !line.contains("Date"))
				{
					InputSplit inputSplit = context.getInputSplit();
					String filename = ((FileSplit)inputSplit).getPath().getName();
					filename = filename.substring(0, filename.indexOf("."));
					
					String[] current = line.split(",");
					key1.set(filename);
					
					value1.set(current[0]+","+current[6]);
					context.write(key1, value1);
				}
		}
	}
	
	
	
	public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
		
		private static String lastVal = null;
		Text key1 = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
			try{
				key1.set(key);
				Text text = new Text();
				Iterator<Text> it = values.iterator();
				double start = 0.0;
				double end = 0.0;
				int month = 1;
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date date = null;
				//String lastVal = null;
				Map<Integer , Double> map = new LinkedHashMap<Integer, Double>();
				lastVal = null;
				
				String[] vals = it.next().toString().split(",");
				date = dateFormat.parse(vals[0]);
				month = date.getMonth();
				start = Double.parseDouble(vals[1]);
				map.put(month, start);
				
				while(it.hasNext()){
					vals = it.next().toString().split(",");
					date = dateFormat.parse(vals[0]);
					
					if( month != date.getMonth() ){
						
						if( lastVal != null ){
							end = Double.parseDouble(lastVal);
							
							//Calculate
							double rateOfReturn = 0.0;
							if( start > 0.0 )
								rateOfReturn = (end - start)/start;
							
							//text.set(String.valueOf(rateOfReturn)+","+String.valueOf(start)+","+String.valueOf(end));
							text.set(String.valueOf(rateOfReturn));
							context.write(key, text);
						}
						
						
						start = Double.parseDouble(vals[1]);
						map.put(month, start);
						
						month = date.getMonth();
					}
					
					lastVal = vals[1];
					
					if( !it.hasNext() ){
						end = Double.parseDouble(lastVal);
						double rateOfReturn = 0.0;
						if( start > 0.0 )
							rateOfReturn = (end - start)/start;
						
						//text.set(String.valueOf(rateOfReturn)+","+String.valueOf(start)+","+String.valueOf(end));
						text.set(String.valueOf(rateOfReturn));
						context.write(key, text);
					}
				}
			}
			catch(ParseException e){
				e.printStackTrace();
			}
		}
	}
	
}
