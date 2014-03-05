package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent last month's time window
 * (i.e. if today is 02/02/2014 last month is 01/2014)
 * @author Daniele Morgantini
 */
public class MonthsAgo implements FixedTime {
	
	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM");
	
	private long from;
	
	private long to;
	
	/** 
	 * No arguments constructor
	 * @return a LastMonth instance
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
	 * Computes the milliseconds of a day in last month
	 * @return the milliseconds of now, one month ago
	 */
	private void setFromAndTo(int n) {		
		Calendar now = Calendar.getInstance();
		now.setTimeInMillis(System.currentTimeMillis());
		this.to = now.getTimeInMillis();
		now.add(Calendar.MONTH, -n);// n months ago
		this.from = now.getTimeInMillis();
	}
	
	@Override
	public String generateRowKey(final long id) {
		return id + "_" + dateFormatter.format(new Date(to));
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