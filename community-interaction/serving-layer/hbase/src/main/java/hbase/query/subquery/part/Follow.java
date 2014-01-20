package hbase.query.subquery.part;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.impls.HBaseClientFactory;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.subquery.HSubQueryComposed;
import hbase.query.subquery.HSubQueryPart;

/**
 * Simple class to represent a "whose followers" subquery part
 * @author Daniele Morgantini
 */
public class Follow extends HSubQueryPart{
			
	private Author author;
	/**
	 * Creates an instance of WhoseFollowers subquery part and attaches it to the belonging HSubQuery
	 * @return a WhoseFollowers instance
	 * @param subQuery the subquery the subquery part belongs to
	 */
	public Follow(final HSubQueryComposed subQuery, Author author) {
		super(subQuery, HBaseClientFactory.getInstance().getFollowedBy());
		this.author = author;
	}
	
	
	public Author getAuthor() {
		return author;
	}


	public void setAuthor(Author author) {
		this.author = author;
	}


	/**
	 * Execute the subquery part
	 * @param authors the authors against who execute the subquery part
	 */
	public Authors execute(final Authors authors) throws IOException {
		
		byte[] id = Bytes.toBytes(this.author.getId());
		List<Author> followers = new ArrayList<Author>();
		Result result = this.getClient().get(id);
		
		for(KeyValue kv : result.raw()) {
			Author a = new Author(Long.parseLong(Bytes.toString(kv.getQualifier())));
			followers.add(a);
		}
		authors.setAuthors(followers);
		return authors;
	}

}
