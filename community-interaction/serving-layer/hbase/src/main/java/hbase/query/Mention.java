package hbase.query;

import hbase.query.Author;

/** 
 * Simple class to represent the action of being mentioned by someone
 * @author Daniele Morgantini
 */
public class Mention {

	private Author mentioned;
	
	/**
	 * Creates an instance of Mention
	 * @param author the author being mentioned
	 */
	public Mention(final Author author) {
		this.mentioned = author;
	}
	
	/**
	 * Creates an instance of Mention by key (author id)
	 * @param id the id of the author being mentioned
	 */
	public Mention(final long id) {
		this.mentioned = new Author(id);
	}
	
	/**
	 * Retrieves the mentioned author
	 * @return the author being mentioned
	 */
	public Author getMentioned() {
		return this.mentioned;
	}
	
	/**
	 * Sets the mentioned in the mention
	 * @param mentioned the author being mentioned
	 */
	public void setMentioned(final Author mentioned) {
		this.mentioned = mentioned;
	}

}
