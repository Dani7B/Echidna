package hbase.query.time;

public class TimeRange {

	private long start;
	private long end;
	
	public TimeRange() {
	}
	
	public TimeRange(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

}
