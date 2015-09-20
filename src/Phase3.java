import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Phase3 {
	public static class Map3 extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		
		//private Text key1 = new Text();
		private DoubleWritable key1 = new DoubleWritable();
		private Text value1 = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] vals = line.split("\t");
			key1.set(Double.parseDouble(vals[1]));
			value1.set(vals[0]);
			context.write(key1, value1);
			
		}
	}
	
	
	public static class Reduce3 extends Reducer<DoubleWritable, Text, Text, Text>{
		private static int startCounter = 1;
		private static Map<String, Double> map1 = new LinkedHashMap<String, Double>();
		private static Map<String, Double> map2 = new LinkedHashMap<String, Double>();
		private static Map<String, Double> map = new LinkedHashMap<String, Double>();
		private static String lastKey = null;
		private Text value1 = new Text();
		
		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			
			/*value1.set("Lowest 5");
			Text key1 = new Text();
			key1.set("");
			context.write(key1, value1);*/
			
			for(Text val: values){
				
				if( startCounter <= 10 ){
					value1.set(key.toString());
					
					map.put(val.toString(), Double.parseDouble(key.toString()));
					//context.write( val , value1 );
				}
				
				if( map1.size() >= 10 ){
					map2 = null;
					map2 = new LinkedHashMap<String, Double>();
					map2.putAll(map1);
					map1 = null;
					map1 = new LinkedHashMap<String, Double>();
				}
				
				map1.put(val.toString(), Double.parseDouble(key.toString()));
				
				lastKey = val.toString();
				
				startCounter++;
			}
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			
			Text key1 = new Text();
			Text value1 = new Text();
			
			key1.set("*********Stocks With Lowest 10 Volatility*********");
			
			context.write(key1, value1);
			
			Iterator<Entry<String, Double>> it0 = map.entrySet().iterator();
			while(it0.hasNext()){
				Entry<String , Double> obj = it0.next();
				key1.set(obj.getKey());
				value1.set(obj.getValue().toString());
				context.write(key1, value1);
			}
			
			
			key1.set("\n*********Stocks With Highest 10 Volatility*********\n");
			
			context.write(key1, value1);
			
			int counter = 0;
			/*if( map1.size() > 0 ){
				Iterator<Entry<String, Double>> it = map1.entrySet().iterator();
				
				while( it.hasNext() ){
					counter++;
					Entry<String , Double> obj = it.next();
					key1.set(obj.getKey());
					value1.set(obj.getValue().toString());
					context.write(key1, value1);
				}
			}*/
			
			//if( counter < 10 ){
				Iterator<Entry<String, Double>> it = map2.entrySet().iterator();
				int localCounter = 0;
				
				while( it.hasNext() ){
					Entry<String , Double> obj = it.next();
					localCounter++;
					if( localCounter > map1.size() ){
						key1.set(obj.getKey());
						value1.set(obj.getValue().toString());
						context.write(key1, value1);
					}
				}
				
				if( map1.size() > 0 ){
					Iterator<Entry<String, Double>> it1 = map1.entrySet().iterator();
					
					while( it1.hasNext() ){
						counter++;
						Entry<String , Double> obj = it1.next();
						key1.set(obj.getKey());
						value1.set(obj.getValue().toString());
						context.write(key1, value1);
					}
				}
			//}
		}
	}
}
