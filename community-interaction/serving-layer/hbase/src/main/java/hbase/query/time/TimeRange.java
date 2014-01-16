package hbase.query.time;

import hbase.HBaseClient;

/**
 * Simple class to represent a time range
 * @author Daniele Morgantini
 */
public abstract class TimeRange {

	private long start;
	private long end;
	
	/**
	 * No arguments constructor
	 */
	public TimeRange() {
	}
	
	
	/**
	 * Creates a TimeRange instance
	 * @return the TimeRange instance
	 * @param start the lower extreme of the time window
	 * @param end the upper extreme of the time window
	 * */
	public TimeRange(final long start, final long end) {
		this.start = start;
		this.end = end;
	}

	/**
	 * Retrieves the lower extreme of the time window
	 * @return the lower extreme of the time window
	 */
	public long getStart() {
		return start;
	}

	
	/**
	 * Sets the lower extreme of the time window
	 * @param start the lower extreme of the time window
	 */
	public void setStart(final long start) {
		this.start = start;
	}

	/**
	 * Retrieves the upper extreme of the time window
	 * @return the upper extreme of the time window
	 */
	public long getEnd() {
		return end;
	}

	/**
	 * Sets the upper extreme of the time window
	 * @param end the upper extreme of the time window
	 */
	public void setEnd(final long end) {
		this.end = end;
	}
	
	public abstract HBaseClient chooseHBaseClient();
	
	public abstract String generateFirstRowKey(final long id);
	
	public abstract String generateLastRowKey(final long id);
}
