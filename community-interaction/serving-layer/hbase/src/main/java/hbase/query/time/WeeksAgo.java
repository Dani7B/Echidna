package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent n weeks ago time window
 * @author Daniele Morgantini
 */
public class WeeksAgo implements FixedTime {
	
	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	private long from;
	
	private long to;
	
	/** 
	 * Creates a WeeksAgo instance
	 * @return the WeeksAgo instance
	 * @param weeks the number of weeks ago to consider
	 */
	public WeeksAgo(int weeks) {
		super();
		this.setFromAndTo(weeks);
	}
	
	/**
	 * Retrieves the start of the time window
	 * @return the start of the time window
	 * */
	public long getStart() {
		return this.from;
	}
		
	/**
	 * Retrieves the end of the time window
	 * @return the end of the time window
	 * */
	public long getEnd() {
		return this.to;
	}
	
	/** Retrieves the string version of the n weeks ago period
	 * @return the string version of the n weeks ago period 
	 */
	public String toString() {
		return dateFormatter.format(new Date(from)) + "_" + dateFormatter.format(new Date(to));
	}
	
	/**
	 * Sets the starting and ending point of the time window
	 */
	private void setFromAndTo(int n) {		
		Calendar now = Calendar.getInstance();
		now.setTimeInMillis(System.currentTimeMillis());
		this.to = now.getTimeInMillis();
		now.add(Calendar.WEEK_OF_YEAR, -n);// n weeks ago
		this.from = now.getTimeInMillis();
	}

	@Override
	public String generateFirstRowKey(final long id) {
		return id + "_" + dateFormatter.format(new Date(from));
	}

	@Override
	public String generateLastRowKey(final long id) {
		return id + "_" + dateFormatter.format(new Date(to));
	}
}