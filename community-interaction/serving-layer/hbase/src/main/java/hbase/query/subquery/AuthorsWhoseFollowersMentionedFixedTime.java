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
import hbase.query.time.FixedTime;
import hbase.query.time.LastMonth;
import hbase.query.time.MonthsAgo;

/**
 * Subquery to represent the authors-whose-followers-mentioned request in a fixed time window
 * @author Daniele Morgantini
 */
public class AuthorsWhoseFollowersMentionedFixedTime extends AuthorsWhoseFollowersMentioned {
	
	private HBaseClient client;
	
	private FixedTime timeRange;
				
	
	/**
	 * Creates an instance of AuthorsWhoseFollowersMentionedFixedTime subquery
	 * @return an instance of AuthorsWhoseFollowersMentionedFixedTime subquery
	 * @param query the belonging query
	 * @param timeRange the fixed time window to take into account
	 * @param atLeast the minimum number of authors to mention
	 * @param times the minimum number of mentions per author
	 * @param mentions the mentions of authors
	 */
	public AuthorsWhoseFollowersMentionedFixedTime(final HQuery query, final FixedTime timeRange,
							final AtLeast atLeast, final AtLeastTimes times, final Mention...mentions) {
		super(query, atLeast, times, mentions);
		this.timeRange = timeRange;
		if(timeRange instanceof LastMonth || timeRange instanceof MonthsAgo)
			this.client = HBaseClientFactory.getInstance().getWhoseFollowersMentionedByMonth();
		else
			this.client = HBaseClientFactory.getInstance().getWhoseFollowersMentionedByDay();

	}
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		Map<String,Integer> map = new HashMap<String,Integer>();
		int mentionMin = this.getAtLeast().getLowerBound();
		int minMentionsPerAuth = this.getAtLeastTimes().getTimes();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(String.valueOf(a.getId()) + "_");
			i++;
		}
				
		for(Mention m : this.getMentions()){
			
			String firstRow = this.timeRange.generateFirstRowKey(m.getMentioned().getId());
			String lastRow = this.timeRange.generateLastRowKey(m.getMentioned().getId());
			
			
			Result[] results = this.client.scanPrefix(Bytes.toBytes(firstRow), Bytes.toBytes(lastRow), auths, Bytes.toBytes(minMentionsPerAuth));
			
			
			for(Result result : results) {
				for(KeyValue kv : result.raw()) {
					int value = 1;
					String mentioner = Bytes.toString(kv.getQualifier());
					if(map.containsKey(mentioner)) {
						value = map.get(mentioner) + Bytes.toInt(kv.getValue());
					}
					map.put(mentioner, value);
				}
			}
		}
		
		List<Author> result = new ArrayList<Author>();
		for(Map.Entry<String, Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= mentionMin) {
				String user = e.getKey().split("_")[0];
				result.add(new Author(Long.parseLong(user)));
			}
		}
		
		this.getQuery().updateUsers(result);
	}
	
}
