package hbase.query.time;

import java.util.Calendar;

/**
 * Simple class to represent last month's time window
 * @author Daniele Morgantini
 */
public class LastMonth extends TimeRange {

	/** 
	 * No arguments constructor
	 * @return a LastMonth instance
	 */
	public LastMonth() {
		this(System.currentTimeMillis());
	}
	
	/**
	 * Creates a LastMonth instance with a desired upper extreme for the time window
	 * @return a LastMonth instance
	 * @param now the upper extreme of the time window
	 */
	public LastMonth(long now) {
		super(getLastMonthMills(now),now);
	}
	
	
	/**
	 * Computes the lower extreme of the time window
	 * @return the millisecond of the lower extreme of the time window, taking into account the upper extreme
	 * @param now the upper extreme of the time window
	 */
	private static long getLastMonthMills(long now) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(now);
		c.add(Calendar.MONTH, -1);// one month ago
		return c.getTimeInMillis();
	}

}
