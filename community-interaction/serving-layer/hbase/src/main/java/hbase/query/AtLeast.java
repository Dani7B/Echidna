package hbase.query;

/**
 * Simple object to represent the minimum required amount for some given property
 * @author Daniele Morgantini
 */
public class AtLeast {

	private int lowerBound;

	
	/**
	 * Creates an AtLeast instance
	 * @param lowerBound the minimum to allow for a given property
	 */
	public AtLeast(int lowerBound) {
		this.lowerBound = lowerBound;
	}

	
	/**
	 * Retrieves the minimum allowed value
	 * @return the minimum allowed
	 */
	public int getLowerBound() {
		return lowerBound;
	}

	
	/**
	 * Sets the minimum allowed value
	 * @param lowerBound the minimum allowed
	 */
	public void setLowerBound(int lowerBound) {
		this.lowerBound = lowerBound;
	}
	
}
