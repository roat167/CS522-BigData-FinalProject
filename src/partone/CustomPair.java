package partone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomPair implements WritableComparable<CustomPair> {
	public static final Text COUNT_SIGN = new Text("");
	private Text current;
	private Text neighbor;
	
	public CustomPair() {
		current = new Text();
		neighbor = new Text();
	}
	
	public CustomPair(Text current, Text neighbor) {
		this.current = current;
		this.neighbor = neighbor;
	}
	
	public Text getCurrent() {
		return current;
	}

	public Text getNeighbor() {
		return neighbor;
	}	

	@Override
	public void readFields(DataInput in) throws IOException {
		this.current.readFields(in);
		this.neighbor.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.current.write(out);
		this.neighbor.write(out);		
	}

	@Override
	public int compareTo(CustomPair other) {
		int res = this.getCurrent().compareTo(other.getCurrent());
		if( res != 0) {
			return res;
		}
		return this.neighbor.compareTo(other.getNeighbor());
	}

	@Override
	public boolean equals(Object obj) {
		CustomPair p = (CustomPair) obj;
		return this.getCurrent().equals(p.getCurrent()) 
				&& this.getNeighbor().equals(p.getNeighbor());
	}

	@Override
	public int hashCode() {		
		int hash = 7;
		hash = 31 *  hash + (this.current != null ? this.current.hashCode() : 0);
		hash = 31 * hash + (this.neighbor != null ? this.neighbor.hashCode() : 0);
		
		return hash;	
		
	}
	
	@Override
	public String toString() {
		return "(" + this.getCurrent() + ", " + this.neighbor + ")";
	}	
}
	