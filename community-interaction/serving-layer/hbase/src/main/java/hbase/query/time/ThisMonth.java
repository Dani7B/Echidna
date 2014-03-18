package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent the current month's time window
 * @author Daniele Morgantini
 */
public class ThisMonth implements FixedTime {

	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM");

	private String date;
	
	/** 
	 * No arguments constructor
	 * @return a ThisMonth instance
	 */
	public ThisMonth() {
		super();
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		final long oneMonthAgo = c.getTimeInMillis();
		this.date = dateFormatter.format(new Date(oneMonthAgo));
	}
	
	/** Retrieves the string version of this month's date
	 * @return the string version of this month's date
	 */
	public String getDate() {
		return date;
	}

	/** Sets the string version of this month's date
	 * @param date the string version of this month's date to set
	 */
	public void setDate(String date) {
		this.date = date;
	}


	private String generateRowKey(final long id) {
		return id + "_" + date;
	}

	@Override
	public String generateFirstRowKey(long id) {
		return this.generateRowKey(id);
	}

	@Override
	public String generateLastRowKey(long id) {
		return this.generateRowKey(id);
	}

}
