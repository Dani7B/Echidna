package hbase.query;

/**
 * Simple object to represent the minimum required number of followers
 * @author Daniele Morgantini
 */
public class AtLeastFollowers {

	private int minFollowers;

	
	/**
	 * Creates an AtLeastFollowers instance
	 * @param minFollowers the minimum to allow for a given property
	 * @return the AtLeastFollowers instance
	 */
	public AtLeastFollowers(final int minFollowers) {
		this.minFollowers = minFollowers;
	}

	
	/**
	 * Retrieves the minimum allowed value for the number of followers
	 * @return the minimum allowed value for the number of followers
	 */
	public int getMinFollowers() {
		return minFollowers;
	}

	
	/**
	 * Sets the minimum allowed value
	 * @param minFollowers the minimum allowed value for the number of followers
	 */
	public void setMinFollowers(final int minFollowers) {
		this.minFollowers = minFollowers;
	}
	
}
