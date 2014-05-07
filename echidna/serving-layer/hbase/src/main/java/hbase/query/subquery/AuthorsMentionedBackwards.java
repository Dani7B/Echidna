package hbase.query.subquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.coprocessors.AuthorAggregatorProtocol;

import hbase.HBaseClient;
import hbase.impls.HBaseClientFactory;
import hbase.query.AtLeastTimes;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.TimeRange;

/**
 * Subquery to represent the very specific and precise authors-mentioned request
 * @author Daniele Morgantini
 */
public class AuthorsMentionedBackwards extends AuthorsMentioned {
	
	private HBaseClient client;
	
	private TimeRange timeRange;
		
	
	/**
	 * Creates an instance of AuthorsMentionedBackwards subquery
	 * @return an instance of AuthorsMentionedBackwards subquery
	 * @param query the belonging query
	 * @param timeRange the specific time window to take into account
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions of authors
	 */
	public AuthorsMentionedBackwards(final HQuery query, final TimeRange timeRange,
									final AtLeastTimes times, final Mention...mentions) {
		
		super(query, times, mentions);
		this.timeRange = timeRange;
		this.client = HBaseClientFactory.getInstance().getMentionedBy();
	}
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		final long lowerBound = this.timeRange.getStart();
		final long upperBound = this.timeRange.getEnd();
		final int minTimes = this.getAtLeastTimes().getTimes();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(a.getId());
			i++;
		}
		
		long min = Long.MAX_VALUE;
		long max = 0;
				
		Map<Long,List<String>> toPass = new HashMap<Long,List<String>>();
		for(Mention m : this.getMentions()) {
			List<String> lista = new ArrayList<String>();
			long id = m.getMentioned().getId();
			lista.add(this.timeRange.generateFirstRowKey(id));
			lista.add(this.timeRange.generateLastRowKey(id));
			toPass.put(id, lista);
			if(id < min)
				min = id;
			if(id > max)
				max = id;
		}
		
		String minRowKey = this.timeRange.generateFirstRowKey(min);
		String maxRowKey = this.timeRange.generateLastRowKey(max);
		
		Batch.Call<AuthorAggregatorProtocol, Map<String,Integer>> call = null;
		try {
			call = Batch.forMethod(AuthorAggregatorProtocol.class,
						"aggregateMentionsBackwards", auths, lowerBound, upperBound, toPass);
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
		
		Map<byte[], Map<String, Integer>> results = null;
		try {
			results = this.client.coprocessorExec(AuthorAggregatorProtocol.class,
					Bytes.toBytes(minRowKey), Bytes.toBytes(maxRowKey), call);
		} catch (Throwable e1) {
			e1.printStackTrace();
		}
		
		Map<String,Integer> aggregation = new HashMap<String,Integer>();
		for (Map.Entry<byte[], Map<String,Integer>> entry : results.entrySet()) {
			for(Map.Entry<String,Integer> res : entry.getValue().entrySet()) {
				String mentioned = res.getKey();
				int count = res.getValue();
				if(aggregation.containsKey(mentioned)){
					count += aggregation.get(mentioned);
				}
				aggregation.put(mentioned, count);
			}
		}
				
		List<Author> list = new ArrayList<Author>();
		for(Map.Entry<String, Integer> el : aggregation.entrySet()) {
			int counter = el.getValue();
			if(counter >= minTimes) {
				list.add(new Author(Long.parseLong(el.getKey()),counter));
			}
		}
		this.getQuery().updateUsers(list);
	}
}
