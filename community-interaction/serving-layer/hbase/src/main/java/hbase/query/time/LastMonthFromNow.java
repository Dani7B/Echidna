package hbase.query.time;

import hbase.HBaseClient;
import hbase.impls.HBaseClientFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent exact last month's time window
 * @author Daniele Morgantini
 */
public class LastMonthFromNow extends TimeRange{

	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	/** 
	 * No arguments constructor
	 * @return a LastMonthFromNow instance
	 */
	public LastMonthFromNow() {
		this(System.currentTimeMillis());
	}
	
	/**
	 * Creates a LastMonthFromNow instance with a desired upper extreme for the time window
	 * @return a LastMonthFromNow instance
	 * @param now the upper extreme of the time window
	 */
	private LastMonthFromNow(final long now) {
		super(getLastMonthMills(now),now);
	}
	
	
	/**
	 * Computes the lower extreme of the time window
	 * @return the milliseconds of the lower extreme of the time window, taking into account the upper extreme
	 * @param now the upper extreme of the time window
	 */
	private static long getLastMonthMills(final long now) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(now);
		c.add(Calendar.MONTH, -1);// one month ago
		return c.getTimeInMillis();
	}

	@Override
	public HBaseClient chooseHBaseClient() {
		return HBaseClientFactory.getInstance().getMentionedBy();
	}
	
	public String generateFirstRowKey(final long id) {
		return LastMonthFromNow.generateRowKey(id, this.getStart());
	}
	
	public String generateLastRowKey(final long id) {
		return LastMonthFromNow.generateRowKey(id, this.getEnd());
	}
	
	private static String generateRowKey(final long id, final long timestamp) {
		
		return id + "_" + dateFormatter.format(new Date(timestamp));
	}
}
