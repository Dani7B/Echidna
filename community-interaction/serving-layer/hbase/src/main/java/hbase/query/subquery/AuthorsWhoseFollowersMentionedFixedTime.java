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
		
		List<Set<String>> sets = new ArrayList<Set<String>>(this.getMentions().size());
		int mentionMin = this.getAtLeast().getLowerBound();
		int minMentionsPerAuth = this.getAtLeastTimes().getTimes();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(String.valueOf(a.getId()) + "_");
			i++;
		}
		
		for(Mention m : this.getMentions()){
			Set<String> set = new HashSet<String>(); // instantiate set for that mentioned
			
			String firstRow = this.timeRange.generateFirstRowKey(m.getMentioned().getId());
			String lastRow = this.timeRange.generateLastRowKey(m.getMentioned().getId());
			
			if(firstRow.equalsIgnoreCase(lastRow)) {
				Result result;
				if(auths.length==0) {
					result = this.client.get(Bytes.toBytes(lastRow),Bytes.toBytes(minMentionsPerAuth));
				}
				else {
					result = this.client.getPrefix(Bytes.toBytes(lastRow), auths, Bytes.toBytes(minMentionsPerAuth));
				}
				for(KeyValue kv : result.raw()) {
						set.add(Bytes.toString(kv.getQualifier())); // if it's a get, the aggregation has already been made
				}
			}
			
			else {
				Result[] results;
				if(auths.length==0){
					results = this.client.scan(Bytes.toBytes(firstRow), Bytes.toBytes(lastRow));
				}
				else {
					results = this.client.scanPrefix(Bytes.toBytes(firstRow), Bytes.toBytes(lastRow), auths);
				}
				Map<String,Integer> map = new HashMap<String,Integer>();
							
				for(Result result : results) {
					for(KeyValue kv : result.raw()) {
						int value = Bytes.toInt(kv.getValue());
						String mentioner = Bytes.toString(kv.getQualifier());
						if(map.containsKey(mentioner)) {
							value += map.get(mentioner);
						}
						if(value>=minMentionsPerAuth) {
							set.add(mentioner);
						}
						map.put(mentioner, value);
					}
				}
			}
			sets.add(set);
		}
		
		/* All the sets have been populated. */
		/* Sum id-mentioner, compare against mentionMin, take only single ids*/
		
		
		Map<String,Integer> result = new HashMap<String,Integer>();
		Set<Long> extracted = new HashSet<Long>();
		for(Set<String> set : sets) {
			for(String s : set) {
				int value = 1;
				if(result.containsKey(s)) {
					value += result.get(s);
				}
				if(value>=mentionMin){
					String user = s.split("_")[0];
					extracted.add(Long.parseLong(user));
				}
				result.put(s, value);
			}
		}
		
		List<Author> list = new ArrayList<Author>();
		for(Long l : extracted) {
			list.add(new Author(l));
		}
		this.getQuery().updateUsers(list);
	}
	
}
