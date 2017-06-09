package partone;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyStripe {
	// KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class StripeMapper extends
			Mapper<LongWritable, Text, Text, StripeMap> {
		
			
		
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, StripeMap>.Context context)
			throws IOException, InterruptedException {			
			String[] ids = value.toString().split(" ");
			int len = ids.length;

			StripeMap maps = new StripeMap();
			// ignore the first item (customer's name)
			for (int i = 1; i < len; i++) {
				maps.clear();				
				String currentId =  ids[i];
				if (currentId.length() < 1) {
					continue;
				}
					
				for (int j = i + 1; j < len; j++) {
					String neighbor = ids[j];
					if (neighbor.length() < 1) {
						continue;					
					}					

					if (currentId.equals(neighbor))
						break;
					
					IntWritable count = (IntWritable) maps.get(new Text(neighbor));			
					if (count != null) {
						maps.put(new Text(neighbor), new IntWritable(count.get() + 1));
					} else {
						maps.put(new Text(neighbor), new IntWritable(1));
					}					
				}
				
				context.write(new Text(currentId), maps);	
								
			}			
		}
	}

	private static class StripePartitioners extends
			Partitioner<Text, StripeMap> {

		@Override	
		public int getPartition(Text key, StripeMap value, int num) {
			String mapKey = key.toString();
			boolean isKeyNumeric = StringUtils.isNumeric(mapKey);
			if (isKeyNumeric) {
				int number =Integer.parseInt(mapKey);

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
				return key.toString().hashCode() % num;
			}			
		}
	}

	// KEYIN, VALUEIN, KEYOUT, VALUEOUT
	private static class StripeReducer extends
			Reducer<Text, StripeMap, Text, StripeMap> {
//		private MultipleOutputs mulOutputs;
		private int marginal;
				
		@Override
		protected void reduce(Text key, Iterable<StripeMap> values,
				Context context) throws IOException, InterruptedException {
			marginal = 0;
			
			StripeMap coMap = new StripeMap();
			for (StripeMap mapW : values) {				
				sum(mapW, coMap);
			}
				
			//Calculate relative frequency			
			for (Entry<Writable, Writable> mapWritable : coMap.entrySet()) {
				double f = (double) Integer.parseInt(mapWritable.getValue().toString()) / (double) marginal;
				DoubleWritable dWritable = new DoubleWritable();
				dWritable.set(new BigDecimal(f).setScale(2, RoundingMode.HALF_UP).doubleValue());				
				mapWritable.setValue(dWritable);
			}
			
			context.write(key, coMap);

		}	
		
		private void sum(StripeMap StripeMap, StripeMap coMap) {
	        for (Writable key : StripeMap.keySet()) {
	        	IntWritable fromCount = (IntWritable) StripeMap.get(key);
	            
	            if (coMap.containsKey(key)) {
	            	IntWritable count = (IntWritable) coMap.get(key);
	                count.set(count.get() + fromCount.get());	                
	            } else {	            	
	            	coMap.put(key, fromCount);
	            	
	            }
	            marginal += fromCount.get();
	        }
	    }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(MyStripe.class);
	
		FileInputFormat.addInputPath(job, new Path("input"));
		 if(FileSystem.get(conf).isDirectory(new Path("output"))) {
	    	 FileSystem.getLocal(conf).delete(new Path("output"), true);
	    }
		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.setMapperClass(StripeMapper.class);
		job.setReducerClass(StripeReducer.class);

		// set Paritioner
		job.setPartitionerClass(StripePartitioners.class);

		// set Reducer num
		job.setNumReduceTasks(4);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StripeMap.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
