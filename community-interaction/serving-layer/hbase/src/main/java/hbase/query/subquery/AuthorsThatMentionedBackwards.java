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
import hbase.query.AtLeastTimes;
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
		
		super(query, atLeast, new AtLeastTimes(1), mentions);
		this.timeRange = timeRange;
		this.client = HBaseClientFactory.getInstance().getMentionedBy();
	}
	
	/**
	 * Creates an instance of AuthorsThatMentionedBackwards subquery
	 * @return an instance of AuthorsThatMentionedBackwards subquery
	 * @param query the belonging query
	 * @param timeRange the specific time window to take into account
	 * @param atLeast the minimum number of authors to mention
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions of authors
	 */
	public AuthorsThatMentionedBackwards(final HQuery query, final TimeRange timeRange,
									final AtLeast atLeast, final AtLeastTimes times, final Mention...mentions) {
		super(query, atLeast, times, mentions);
		this.timeRange = timeRange;
		this.client = HBaseClientFactory.getInstance().getMentionedBy();
	}
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		Map<Long,Integer> general = new HashMap<Long,Integer>();
		final long lowerBound = this.timeRange.getStart();
		final long upperBound = this.timeRange.getEnd();
		final int minMentionedAuthors = this.getAtLeast().getLowerBound();
		final int minTimes = this.getAtLeastTimes().getTimes();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(a.getId());
			i++;
		}
		
				
		for(Mention m : this.getMentions()){
			
			Map<Long, Integer> counter = new HashMap<Long, Integer>();
			String lowerRow = this.timeRange.generateFirstRowKey(m.getMentioned().getId());
			String upperRow = this.timeRange.generateLastRowKey(m.getMentioned().getId());
						
			Result[] results = this.client.scan(Bytes.toBytes(lowerRow),Bytes.toBytes(upperRow),
												Bytes.toBytes(String.valueOf(lowerBound)),
												Bytes.toBytes(String.valueOf(upperBound)),
												auths);
			
			for(Result res : results) {
				for(KeyValue kv : res.raw()) {
					Long mentioner = Bytes.toLong(kv.getValue());
					int value = 1;
					if(counter.containsKey(mentioner)) {
						value += counter.get(mentioner);
					}
					counter.put(mentioner, value);
				}
			}
			
			for(Map.Entry<Long, Integer> e : counter.entrySet()){
				if(e.getValue()>= minTimes){
					Long key = e.getKey();
					int value = 1;
					if(general.containsKey(key)) {
						value += general.get(key);
					}
					general.put(key, value);
				}
			}
		}
		
		List<Author> users = new ArrayList<Author>();
		for(Map.Entry<Long, Integer> el : general.entrySet()) {
			int value = el.getValue();
			if(value >= minMentionedAuthors) {
				users.add(new Author(el.getKey(),value));
			}
		}
		
		this.getQuery().updateUsers(users);
	}
	
}
