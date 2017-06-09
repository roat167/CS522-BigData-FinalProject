package partone;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyPair {
	//KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class PairMapper extends Mapper<LongWritable, Text, CustomPair, IntWritable> {
		HashMap<CustomPair, Integer> maps;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, CustomPair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			maps = new HashMap<CustomPair, Integer>();
			
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, CustomPair, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] ids = value.toString().split(" ");
			int len = ids.length;
			
			//ignore the first item (customer's name)
			for (int i = 1; i < len ; i++) {
				String currentId =  ids[i];
				if (currentId.isEmpty()) continue;				
				
				for (int j = i + 1; j < len ; j++) {
					String neighbor = ids[j];
					if (neighbor.length() < 1) continue;					
					
					if (currentId.equals(neighbor)) break;
					
					CustomPair pair = new CustomPair(new Text(currentId), new Text(neighbor));
					Integer count = maps.get(pair);
					
					if (count != null ) {
						count += 1;
					} 
					else {
						count = 1;
					}
					
					maps.put(pair, count);
					
					//add additional star sign to map
					pair = new CustomPair(new Text(currentId), CustomPair.COUNT_SIGN);
					count = maps.get(pair);
					
					if (count != null ) {
						count += 1;
					} 
					else {
						count = 1;
					}
					maps.put(new CustomPair(new Text(currentId), CustomPair.COUNT_SIGN), count);
				}
			}
			
		}	
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, CustomPair, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Entry<CustomPair, Integer> entry : maps.entrySet()) {
				IntWritable count = new IntWritable(entry.getValue());	
				context.write(entry.getKey(), count);					
			}
		}
	}
	
	private static class PairPartitioner extends Partitioner<CustomPair, IntWritable> {

		@Override
		public int getPartition(CustomPair p, IntWritable value, int num) {
			String key = p.getCurrent().toString();
			boolean isKeyNumeric = StringUtils.isNumeric(key);
			if (isKeyNumeric) {
				int number =Integer.parseInt(key);

				if (number < 20) {
					return 0 % num;					
				}
				if (number < 50) {
					return 1 % num;
				}
				if (number < 500) {
					return 2 % num;
				}
				return 3 % num;
				
			} else {				
				return p.getCurrent().toString().hashCode() % num;
			}			
		}		
	}
	
	
	//KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class PairReducer extends Reducer<CustomPair, IntWritable, CustomPair, DoubleWritable> {
		private DoubleWritable marginal = new DoubleWritable();
		private DoubleWritable relFreq = new DoubleWritable();			
					
		@Override
		protected void reduce(
				CustomPair key,
				Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			
			for(IntWritable val : values) {
				sum += val.get();
				
				if (key.getNeighbor().equals(CustomPair.COUNT_SIGN)) {				
					marginal.set(sum);					
				} 
				else {
					double res = sum / marginal.get();					
					relFreq.set(new BigDecimal(res).setScale(2, RoundingMode.HALF_UP).doubleValue());
					context.write(key, relFreq);
				}	
				
			}			
			
		}		
	}	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    
	    job.setJarByClass(MyPair.class);
	    
	    FileInputFormat.addInputPath(job, new Path("input"));
	    FileOutputFormat.setOutputPath(job, new Path("output"));
	    
	    job.setMapperClass(PairMapper.class);
	    job.setReducerClass(PairReducer.class);
	    	
	    //set Paritioner
	    job.setPartitionerClass(PairPartitioner.class);
	    
	    //set Reducer num
	    job.setNumReduceTasks(4);
	    
	    job.setOutputKeyClass(CustomPair.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	     
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
}
