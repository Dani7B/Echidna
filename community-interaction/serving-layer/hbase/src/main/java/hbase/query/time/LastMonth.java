package hbase.query.time;

import java.util.Calendar;

public class LastMonth extends TimeRange {

	public LastMonth() {
		this(System.currentTimeMillis());
	}
	
	public LastMonth(long now) {
		super(getLastMonthMills(now),now);
	}
	
	private static long getLastMonthMills(long now) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(now);
		c.add(Calendar.MONTH, -1);// one month ago
		return c.getTimeInMillis();
	}

}
