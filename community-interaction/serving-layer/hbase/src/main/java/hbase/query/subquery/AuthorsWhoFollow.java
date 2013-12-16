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
import hbase.query.AtLeast;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

public class AuthorsWhoFollow extends HSubQuery {

	private HBaseClient client;
	
	private AtLeast atLeast;
	
	private List<Author> followed;
	
	public AuthorsWhoFollow(HQuery query, HBaseClient client, AtLeast atLeast, Author... authors) {
		super(query);
		this.client = client;
		this.atLeast = atLeast;
		this.followed = new ArrayList<Author>();
		for(Author a : authors)
			this.followed.add(a);
	}

	@Override
	public void execute(Authors authors) throws IOException {
		
		byte[][] columns = new byte[authors.getAuthors().size()][];
		
		int i = 0;
		for(Author a : authors.getAuthors()) {
			columns[i] = Bytes.toBytes(a.getId());
			i++;
		}
		
		Map<byte[],Integer> map = new HashMap<byte[],Integer>();
		
		for(Author a : this.followed) {
			byte[] aID = Bytes.toBytes(a.getId());
			Result result = this.client.get(aID, columns);
			
			for(KeyValue kv : result.raw()) {
				byte[] id = kv.getQualifier();
				int value = 1;
				if(map.containsKey(id))
					value = map.get(id) + 1;
				map.put(id, value);
			}
		}
		
		List<Author> result = new ArrayList<Author>();
		int followMin = this.atLeast.getLowerBound();
		
		for(Map.Entry<byte[], Integer> e : map.entrySet()) {
			if(e.getValue() >= followMin)
				result.add(new Author(Bytes.toLong(e.getKey())));
		}
		
		authors.setAuthors(result);
	}

}
