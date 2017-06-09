package partone;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StripeMap extends MapWritable implements WritableComparable<StripeMap>{
	@Override
	public String toString() {	
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		try {
			Map<Writable, Writable> treeMap = new TreeMap<Writable, Writable> (this);
			for (Map.Entry<Writable, Writable> map : treeMap.entrySet()) {
				sb.append(map.getKey()).append(":");			
				double val = Double.parseDouble(((Writable) map.getValue()).toString())	;
				sb.append(new BigDecimal(val).setScale(2, RoundingMode.HALF_UP).toString()).append(", ");			
			}
			sb.append("}");			
		} catch( Exception e) {
			e.printStackTrace();				
		}
		String st = sb.toString().replaceAll(", }", "}");
		return st;
	}

	@Override
	public int compareTo(StripeMap other) {		
		return this.compareTo(other);		
	}
	
}
