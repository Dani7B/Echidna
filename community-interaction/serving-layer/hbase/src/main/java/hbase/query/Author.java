package hbase.query;

/**
 * Simple class to represent an Author
 * @author Daniele Morgantini
 */
public class Author {

	private long id;
	
	/**
	 * Creates an instance of Author
	 * @param id the author id
	 */
	public Author(final long id) {
		this.id = id;
	}

	
	/**
	 * Retrieves the author id
	 * @return the author id
	 */
	public long getId() {
		return id;
	}

	
	/**
	 * Sets the author id
	 * @param id the author id
	 */
	public void setId(final long id) {
		this.id = id;
	}
	
	/**
	 * Retrieve the string representation of the Author
	 * @return the string representation of the Author
	 */
	public String toString() {
		return "" + this.id;
	}
}
