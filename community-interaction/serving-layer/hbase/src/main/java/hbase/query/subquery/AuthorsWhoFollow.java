package hbase.query.subquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.HBaseClient;
import hbase.impls.HBaseClientFactory;
import hbase.query.AtLeast;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Subquery to represent the authors-who-follow request
 * @author Daniele Morgantini
 */
public class AuthorsWhoFollow extends HSubQuery {

	private HBaseClient client;
	
	private AtLeast atLeast;
	
	private List<Author> followed;
	
	
	/**
	 * Creates an instance of AuthorsWhoFollow subquery
	 * @return an instance of AuthorsWhoFollow subquery
	 * @param query the belonging query
	 * @param atLeast the minimum number of authors to follow
	 * @param authors the followed authors
	 */
	public AuthorsWhoFollow(final HQuery query,	final AtLeast atLeast, final Author... authors) {
		super(query);
		this.client = HBaseClientFactory.getInstance().getFollowedBy();
		this.atLeast = atLeast;
		this.followed = new ArrayList<Author>();
		for(Author a : authors)
			this.followed.add(a);
	}

	@Override
	public void execute(final Authors authors) throws IOException {
		
		byte[][] columns = new byte[authors.getAuthors().size()][];
		
		int i = 0;
		for(Author a : authors.getAuthors()) {
			columns[i] = Bytes.toBytes(String.valueOf(a.getId()));
			i++;
		}
		
		Map<Long,Integer> map = new HashMap<Long,Integer>();
		
		for(Author a : this.followed) {
			byte[] aID = Bytes.toBytes(a.getId());
			Result result = null;
			if(authors.isEmpty()) {
				result = this.client.get(aID);
			}
			else {
				result = this.client.get(aID, columns);
			}
			
			for(KeyValue kv : result.raw()) {
				long id = Long.valueOf(Bytes.toString(kv.getQualifier()));
				int value = 1;
				if(map.containsKey(id))
					value = map.get(id) + 1;
				map.put(id, value);
			}
		}
		
		List<Author> result = new ArrayList<Author>();
		int followMin = this.atLeast.getLowerBound();
		
		for(Map.Entry<Long, Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= followMin)
				result.add(new Author(e.getKey(), value));
		}
		
		this.getQuery().updateUsers(result);
	}

}
