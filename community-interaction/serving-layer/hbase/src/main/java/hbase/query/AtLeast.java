package hbase.query;

public class AtLeast {

	private int lowerBound;

	public AtLeast(int lb) {
		
		this.lowerBound = lb;
	}

	public int getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(int lowerBound) {
		this.lowerBound = lowerBound;
	}
	
	
}
