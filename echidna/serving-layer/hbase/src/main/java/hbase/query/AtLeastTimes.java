package hbase.query;

/**
 * Simple object to represent the minimum required amount for the counter of repetitive action
 * @author Daniele Morgantini
 */
public class AtLeastTimes {

	private int times;

	
	/**
	 * Creates an AtLeastTimes instance
	 * @param times the minimum to allow for a given property
	 * @return the AtLeastTimes instance
	 */
	public AtLeastTimes(final int times) {
		this.times = times;
	}

	
	/**
	 * Retrieves the minimum allowed value
	 * @return the minimum allowed value
	 */
	public int getTimes() {
		return times;
	}

	
	/**
	 * Sets the minimum allowed value
	 * @param times the minimum allowed value
	 */
	public void setTimes(final int times) {
		this.times = times;
	}
	
}
