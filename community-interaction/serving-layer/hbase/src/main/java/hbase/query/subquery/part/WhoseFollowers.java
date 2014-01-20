package hbase.query.subquery.part;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class WhoseFollowers extends HSubQueryPart{
	
	private Authors authors;
		
	/**
	 * Creates an instance of WhoseFollowers subquery part and attaches it to the belonging HSubQuery
	 * @return a WhoseFollowers instance
	 * @param subQuery the subquery the subquery part belongs to
	 */
	public WhoseFollowers(final HSubQueryComposed subQuery, final Authors authors) {
		super(subQuery, HBaseClientFactory.getInstance().getFollow());
		this.authors = authors;
	}

	public Authors follow(Author author) {
		HSubQueryPart part = new Follow(this.getSubQuery(), author);
		return this.authors;
	}
	
	/**
	 * Execute the subquery part
	 * @param authors the authors against who execute the subquery part
	 */
	public Authors execute(final Authors authors) throws IOException {
		
		Map<Long,Author> map = new HashMap<Long,Author>();
		
		for(Author a : authors.getAuthors()) {
			byte[] id = Bytes.toBytes(a.getId());
			
			Result result = this.getClient().get(id);
			
			for(KeyValue kv : result.raw()) {
				long idFollowed = Long.parseLong(Bytes.toString(kv.getQualifier()));
				Author auth = null;
				if(map.containsKey(idFollowed)) {
					auth = map.get(idFollowed);
					auth.incrementHits();
				}
				else {
					auth = new Author(idFollowed);
				}
				map.put(idFollowed, auth);
			}
		}
		
		List<Author> list = new ArrayList<Author>(map.values());
		authors.setAuthors(list);
		this.authors = authors;
		return this.authors;
	}

}
