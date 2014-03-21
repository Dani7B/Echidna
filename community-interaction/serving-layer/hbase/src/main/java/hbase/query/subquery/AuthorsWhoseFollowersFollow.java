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
import hbase.query.AtLeastFollowers;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Subquery to represent the authors-whose-followers-follow request.
 * The result is computed considering the followed authors in OR.
 * @author Daniele Morgantini
 */
public class AuthorsWhoseFollowersFollow extends HSubQuery {

	private HBaseClient client;
		
	private List<Author> followed;
	
	private AtLeast atLeast;
	
	private AtLeastFollowers minFollowers;
	
	/**
	 * Creates an instance of AuthorsWhoseFollowersFollow subquery
	 * @return an instance of AuthorsWhoseFollowersFollow subquery
	 * @param query the belonging query
	 * @param atLeast the minimum number of followed authors
	 * @param minFollowers the minimum number of followers who follow a user
	 * @param authors the followed authors
	 */
	public AuthorsWhoseFollowersFollow(final HQuery query, final AtLeast atLeast,
							final AtLeastFollowers minFollowers, final Author... authors) {
		super(query);
		this.client = HBaseClientFactory.getInstance().getWhoseFollowersFollow();
		this.followed = new ArrayList<Author>();
		for(Author a : authors)
			this.followed.add(a);
		this.atLeast = atLeast;
		this.minFollowers = minFollowers;
	}

	@Override
	public void execute(final Authors authors) throws IOException {
		
		byte[][] columns = new byte[authors.getAuthors().size()][];
		int minFollwrs = this.minFollowers.getMinFollowers();
		
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
				result = this.client.get(aID, Bytes.toBytes(minFollwrs));
			}
			else {
				result = this.client.get(aID, columns, Bytes.toBytes(minFollwrs));
			}
			
			for(KeyValue kv : result.raw()) {
				long id = Long.valueOf(Bytes.toString(kv.getQualifier()));
				int value = 1;
				if(map.containsKey(id))
					value += map.get(id);
				map.put(id, value);
			}
		}
		
		List<Author> list = new ArrayList<Author>();
		for(Map.Entry<Long, Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= this.atLeast.getLowerBound()) {
				list.add(new Author(e.getKey(),value));
			}
		}
		this.getQuery().updateUsers(list);
	}

}
