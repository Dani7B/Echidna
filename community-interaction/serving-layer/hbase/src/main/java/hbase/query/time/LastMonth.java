package hbase.query.time;

import hbase.HBaseClient;
import hbase.impls.HBaseClientFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent last month's time window
 * (i.e. if today is 02/02/2014 last month is 01/2014)
 * @author Daniele Morgantini
 */
public class LastMonth implements FixedTime {

	private String date;
	
	/** 
	 * No arguments constructor
	 * @return a LastMonth instance
	 */
	public LastMonth() {
		super();
		final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM");
		final long oneMonthAgo = getLastMonthMills();
		date = dateFormatter.format(new Date(oneMonthAgo));
	}
	
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}


	/**
	 * Computes the milliseconds of a day in last month
	 * @return the milliseconds of now, one month ago
	 */
	private static long getLastMonthMills() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		c.add(Calendar.MONTH, -1);// one month ago
		return c.getTimeInMillis();
	}
	
	
	public HBaseClient chooseHBaseClient() {
		return HBaseClientFactory.getInstance().getMentionedByMonth();
	}
	
	public String generateRowKey(final long id) {
		return id + "_" + date;
	}

}
