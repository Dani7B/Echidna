package hbase.query.subquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.HBaseClient;
import hbase.query.AtLeast;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.TimeRange;

/**
 * Subquery to represent the very specific and precise authors-that-mentioned request
 * @author Daniele Morgantini
 */
public class AuthorsThatMentionedBackwards extends AuthorsThatMentioned {
	
	private HBaseClient client;
	
	private TimeRange timeRange;
		
	
	/**
	 * Creates an instance of AuthorsThatMentionedBackwards subquery
	 * @return an instance of AuthorsThatMentionedBackwards subquery
	 * @param query the belonging query
	 * @param timeRange the specific time window to take into account
	 * @param atLeast the minimum number of authors to mention
	 * @param mentions the mentions of authors
	 */
	public AuthorsThatMentionedBackwards(final HQuery query, final TimeRange timeRange,
									final AtLeast atLeast, final Mention...mentions) {
		super(query, atLeast, mentions);
		this.timeRange = timeRange;
		this.client = this.timeRange.chooseHBaseClient();
	}
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		Map<byte[],Integer> map = new HashMap<byte[],Integer>();
		final long lowerBound = this.timeRange.getStart();
		final long upperBound = this.timeRange.getEnd();
		final int mentionMin = this.getAtLeast().getLowerBound();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(a.getId());
			i++;
		}
		
				
		for(Mention m : this.getMentions()){
			
			String lowerRow = this.timeRange.generateFirstRowKey(m.getMentioned().getId());
			String upperRow = this.timeRange.generateLastRowKey(m.getMentioned().getId());
						
			Result[] results = this.client.scan(Bytes.toBytes(lowerRow),Bytes.toBytes(upperRow),
												Bytes.toBytes(String.valueOf(lowerBound)),
												Bytes.toBytes(String.valueOf(upperBound)),
												auths);
			
			Set<byte[]> singleIDs = new HashSet<byte[]>();
			for(Result res : results){
				for(KeyValue kv : res.raw()){
					singleIDs.add(kv.getValue());
				}
			}
			
			for(byte[] id : singleIDs){
				int value = 1;
				if(map.containsKey(id)) {
					value = map.get(id) + 1;
				}
				map.put(id, value);
			}
		}
		
		List<Author> result = new ArrayList<Author>();
		for(Map.Entry<byte[], Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= mentionMin)
				result.add(new Author(Bytes.toLong(e.getKey())));
		}
		
		this.getQuery().updateUsers(result);
	}
	
}
