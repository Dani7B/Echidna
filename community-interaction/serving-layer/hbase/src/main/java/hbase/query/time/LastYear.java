package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent last year's time window
 * @author Daniele Morgantini
 */
public class LastYear implements FixedTime {

	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy");

	private long from;
	
	private long to;
	
	/** No arguments constructor */
	public LastYear() {
		Calendar now = Calendar.getInstance();
		now.setTimeInMillis(System.currentTimeMillis());
		this.to = now.getTimeInMillis();
		now.add(Calendar.YEAR, -1);// 1 year ago
		this.from = now.getTimeInMillis();
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
	
	@Override
	public String generateFirstRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(this.from));
	}

	@Override
	public String generateLastRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(this.to));
	}

}
