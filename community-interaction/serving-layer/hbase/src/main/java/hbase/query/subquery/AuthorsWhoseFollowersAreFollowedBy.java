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
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Subquery to represent the authors-whose-followers-follow request.
 * The result is computed considering the followers in OR.
 * @author Daniele Morgantini
 */
public class AuthorsWhoseFollowersAreFollowedBy extends HSubQuery {

	private HBaseClient client;
		
	private List<Author> followers;
	
	
	/**
	 * Creates an instance of AuthorsWhoseFollowersFollow subquery
	 * @return an instance of AuthorsWhoseFollowersFollow subquery
	 * @param query the belonging query
	 * @param authors the followed authors
	 */
	public AuthorsWhoseFollowersAreFollowedBy(final HQuery query, final Author... authors) {
		super(query);
		this.client = HBaseClientFactory.getInstance().getWhoseFollowersAreFollowedBy();
		this.followers = new ArrayList<Author>();
		for(Author a : authors)
			this.followers.add(a);
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
		
		for(Author a : this.followers) {
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
				int value = Bytes.toInt(kv.getValue());
				if(map.containsKey(id))
					value += map.get(id);
				map.put(id, value);
			}
		}
		
		List<Author> list = new ArrayList<Author>();
		for(Map.Entry<Long, Integer> e : map.entrySet()) {
			list.add(new Author(e.getKey(), e.getValue()));
		}
		
		this.getQuery().updateUsers(list);
	}

}
