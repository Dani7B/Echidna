package hbase.query.time;

/** Interface to represent a fixed time windows, not too strictly dependent on the current day */
public interface FixedTime {

	/**
	 * Generates the row key based on the time window specified
	 * @param id the id to compute the row key upon
	 * @return the row key
	 */
	public abstract String generateRowKey(final long id);
}
