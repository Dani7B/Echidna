package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent last week's time window
 * @author Daniele Morgantini
 */
public class LastWeek extends TimeRange {

	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	/** No arguments constructor */
	public LastWeek() {
		Calendar now = Calendar.getInstance();
		now.setTimeInMillis(System.currentTimeMillis());
		super.setEnd(now.getTimeInMillis());
		now.add(Calendar.WEEK_OF_YEAR, -1);// 1 week ago
		super.setStart(now.getTimeInMillis());
	}

	/**
	 * Creates a LastWeek instance
	 * @return the LastWeek instance
	 * @param start the lower extreme of the time window
	 * @param end the upper extreme of the time window
	 * */
	public LastWeek(long start, long end) {
		super(start, end);
	}

	@Override
	public String generateFirstRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(super.getStart()));
	}

	@Override
	public String generateLastRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(super.getEnd()));
	}

}
