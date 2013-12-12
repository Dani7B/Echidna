package hbase.query;

import hbase.query.Author;

public class Mention {

	private Author mentioned;
	
	public Mention() {
	}
	
	public Mention(Author author) {
		this.mentioned = author;
	}
	
	public Mention(long id) {
		this.mentioned = new Author(id);
	}
	
	public Author getMentioned() {
		return this.mentioned;
	}
	
	public void setMentioned(Author mentioned) {
		this.mentioned = mentioned;
	}

}
