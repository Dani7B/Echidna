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
import hbase.query.Mention;
import hbase.query.time.FixedTime;

/**
 * Subquery to represent the authors-that-mentioned request in a fixed time window
 * @author Daniele Morgantini
 */
public class AuthorsThatMentionedFixedTime extends AuthorsThatMentioned {
	
	private HBaseClient client;
	
	private FixedTime timeRange;
				
	
	/**
	 * Creates an instance of AuthorsThatMentionedFixedTime subquery
	 * @return an instance of AuthorsThatMentionedFixedTime subquery
	 * @param query the belonging query
	 * @param client the HBaseClient to query HBase
	 * @param timeRange the time window to take into account
	 * @param atLeast the minimum number of authors to mention
	 * @param mentions the mentions of authors
	 */
	public AuthorsThatMentionedFixedTime(final HQuery query, final FixedTime timeRange,
									final AtLeast atLeast, final Mention...mentions) {
		super(query, atLeast, mentions);
		this.timeRange = timeRange;
		this.client = this.timeRange.chooseHBaseClient();
	}
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		Map<byte[],Integer> map = new HashMap<byte[],Integer>();
		int mentionMin = this.getAtLeast().getLowerBound();
		
				
		for(Mention m : this.getMentions()){
			
			String row = this.timeRange.generateRowKey(m.getMentioned().getId());
						
			Result result = this.client.get(row);
			
			for(KeyValue kv : result.raw()) {
				int value = 1;
				byte[] mentioner = kv.getQualifier();
				if(map.containsKey(mentioner)) {
					value = map.get(mentioner) + 1;
				}
				map.put(mentioner, value);
			}
		}
		
		List<Author> result = new ArrayList<Author>();
		for(Map.Entry<byte[], Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= mentionMin)
				result.add(new Author(Long.parseLong(Bytes.toString(e.getKey()))));
		}
		
		this.getQuery().updateUsers(result);
	}
	
}
