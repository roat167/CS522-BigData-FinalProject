package partone;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomMap extends SortedMapWritable implements WritableComparable<CustomMap>{
	@Override
	public String toString() {	
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		try {
			for (Map.Entry<WritableComparable, Writable> map : this.entrySet()) {
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
	public int compareTo(CustomMap other) {		
		return this.compareTo(other);		
	}
	
}
