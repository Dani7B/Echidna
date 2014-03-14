package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent n months ago time window
 * (i.e. if today is 02/03/2014 and n is 2 we get [01/2014,03/2014) )
 * @author Daniele Morgantini
 */
public class MonthsAgo implements FixedTime {
	
	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM");
	
	private long from;
	
	private long to;
	
	/** 
	 * Creates a MonthsAgo instance
	 * @return the MonthsAgo instance
	 * @param months the number of months ago to consider
	 */
	public MonthsAgo(int months) {
		super();
		this.setFromAndTo(months);
	}
	
	/** Retrieves the string version of the n months ago period
	 * @return the string version of the n months ago period 
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
		now.add(Calendar.MONTH, -n);// n months ago
		this.from = now.getTimeInMillis();
	}

	@Override
	public String generateFirstRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(from));
	}

	@Override
	public String generateLastRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(to));
	}
}