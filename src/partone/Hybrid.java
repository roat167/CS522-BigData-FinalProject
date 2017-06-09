package partone;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Hybrid {	
	//KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class HybridMapper extends Mapper<LongWritable, Text, CustomPair, IntWritable> {
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
				if (currentId.length() < 1) continue;				
				
				for (int j = i + 1; j < len ; j++) {
					String neighbor = ids[j];
					if (neighbor.isEmpty()) continue;					
					
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
				}
			}
			System.out.println(maps);
			
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
	
	private static class HybridPartitioner extends Partitioner<CustomPair, IntWritable> {

		@Override
		public int getPartition(CustomPair p, IntWritable value, int num) {
			String key = p.getCurrent().toString();
			boolean isKeyNumeric = StringUtils.isNumeric(key);
			if (isKeyNumeric) {
				int number =Integer.parseInt(key);

				if (number < 20) {
					return 0 % num;					
				}
				else if(number < 50) {
					return 1 % num;
				} 
				else if(number < 90) {
					return 2 % num;
				} 
				else {
					return 3 % num;
				}			
				
			} else {				
				return p.getCurrent().toString().hashCode() % num;
			}			

		}		
	}
	
	
	//KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class HybridReducer extends Reducer<CustomPair, IntWritable, Text, Text> {
		private CustomMap customMap;
		private int marginal;
		private String currentID; 
		
		@Override
		protected void setup(
				Reducer<CustomPair, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {			
			marginal = 0;
			currentID = null;
			customMap = new CustomMap();
		}
		
		@Override
		protected void reduce(CustomPair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String termW = key.getCurrent().toString();			
			
			if (currentID == null) {
				currentID = termW;
			}
			else if (!(termW.equals(currentID))) {
				calRelativeFrequency(customMap);
					
				context.write(new Text(currentID), new Text(customMap.toString()));
				marginal = 0;
				customMap = new CustomMap();
				currentID = termW;
			}
			
			String termU = key.getNeighbor().toString();
			for (IntWritable c : values) {
				if(customMap.containsKey(key.getNeighbor())) {
					customMap.put(new Text(termU), new IntWritable(((IntWritable)customMap.get(key.getNeighbor())).get() + c.get()));
				} else {				
					customMap.put(new Text(termU), new IntWritable(c.get()));
				}
				marginal += c.get();
			}
			
		}
		
		@Override	
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			calRelativeFrequency(customMap);			
			context.write(new Text(currentID), new Text(customMap.toString()));			
		}
		
		private void calRelativeFrequency(CustomMap customMap) {
			for (Entry<WritableComparable, Writable> neighbor : customMap.entrySet()) {
				DoubleWritable dWritable = new DoubleWritable();
				Double f = new Double(((double)((IntWritable) neighbor.getValue()).get()) / (double)marginal);				
				dWritable.set(new BigDecimal(f).setScale(2, RoundingMode.HALF_UP).doubleValue());				
				customMap.put(neighbor.getKey(), dWritable);
			}
		}
	}	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    
	    job.setJarByClass(Hybrid.class);
	    
	    FileInputFormat.addInputPath(job, new Path("input"));	    
	    if(FileSystem.get(conf).isDirectory(new Path("output"))) {
	    	 FileSystem.getLocal(conf).delete(new Path("output"), true);
	    }
	    FileOutputFormat.setOutputPath(job, new Path("output"));	 
	    
	    job.setMapperClass(HybridMapper.class);
	    job.setReducerClass(HybridReducer.class);
	    	
	    //set Paritioner
	    job.setPartitionerClass(HybridPartitioner.class);
	    
	    //set Reducer num
	    job.setNumReduceTasks(4);
	    
	    job.setOutputKeyClass(CustomPair.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	     
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
