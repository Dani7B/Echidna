package hbase.query;

/**
 * Simple class to represent an Author
 * @author Daniele Morgantini
 */
public class Author {

	private long id;
	
	private int hits;
		
	/**
	 * Creates an instance of Author
	 * @param id the author's id
	 */
	public Author(final long id) {
		this.id = id;
		this.hits = 1;
	}

	
	/**
	 * Retrieves the author's id
	 * @return the author's id
	 */
	public long getId() {
		return id;
	}

	
	/**
	 * Sets the author's id
	 * @param id the author's id
	 */
	public void setId(final long id) {
		this.id = id;
	}
	
	
	/**
	 * Retrieves the number of hits for the author
	 * @return the number of hits for the author
	 */
	public int getHits() {
		return hits;
	}

	/**
	 * Sets the number of hits for the author
	 * @param id the number of hits for the author
	 */
	public void setHits(int hits) {
		this.hits = hits;
	}

	/**
	 * Increments by one the number of hits for the author
	 */
	public void incrementHits() {
		this.hits++;
	}
	
	/**
	 * Retrieve the string representation of the Author
	 * @return the string representation of the Author
	 */
	public String toString() {
		return "" + this.id;
	}
}
