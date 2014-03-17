package hbase.query.time;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Simple class to represent this year's time window
 * @author Daniele Morgantini
 */
public class ThisYear implements FixedTime {

	private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy");

	private long now;
	
	
	/** No arguments constructor */
	public ThisYear() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		this.now = c.getTimeInMillis();
	}
	
	@Override
	public String generateFirstRowKey(long id) {
		return id + "_" + dateFormatter.format(new Date(this.now));
	}

	@Override
	public String generateLastRowKey(long id) {
		return generateFirstRowKey(id) ;
	}

}
