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
import hbase.query.AtLeast;
import hbase.query.AtLeastTimes;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.FixedTime;
import hbase.query.time.LastMonth;
import hbase.query.time.LastYear;
import hbase.query.time.MonthsAgo;
import hbase.query.time.ThisYear;

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
	 * @param timeRange the fixed time window to take into account
	 * @param atLeast the minimum number of authors to mention
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions of authors
	 */
	public AuthorsThatMentionedFixedTime(final HQuery query, final FixedTime timeRange,
						final AtLeast atLeast, final AtLeastTimes times, final Mention...mentions) {
		super(query, atLeast, times, mentions);
		this.timeRange = timeRange;
		this.setClient();
	}
	
	private void setClient() {
		if(this.timeRange instanceof LastMonth || this.timeRange instanceof MonthsAgo ||
				this.timeRange instanceof LastYear || this.timeRange instanceof ThisYear)
			this.client = HBaseClientFactory.getInstance().getMentionedByMonth();
		else
			this.client = HBaseClientFactory.getInstance().getMentionedByDay();
	}
	
	/**
	 * Retrieves the timeRange
	 * @return the timeRange
	 */
	public FixedTime getTimeRange() {
		return this.timeRange;
	}
	
	/*
	@Override
	public void execute(final Authors authors) throws IOException {
		
		Map<String,Integer> general = new HashMap<String,Integer>();
		final int minMentionedAuthors = this.getAtLeast().getLowerBound();
		final int minTimes = this.getAtLeastTimes().getTimes();
		
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(String.valueOf(a.getId()));
			i++;
		}
				
		for(Mention m : this.getMentions()){
			Map<String,Integer> map = new HashMap<String,Integer>();
			
			String firstRow = this.timeRange.generateFirstRowKey(m.getMentioned().getId());
			String lastRow = this.timeRange.generateLastRowKey(m.getMentioned().getId());
			Result[] results;
			if(this.timeRange instanceof ThisYear)
				results = this.client.scanPrefix(Bytes.toBytes(firstRow), auths);
			else
				results = this.client.scan(Bytes.toBytes(firstRow), Bytes.toBytes(lastRow), auths);
			
			for(Result result : results) {
				for(KeyValue kv : result.raw()) {
					int value = Bytes.toInt(kv.getValue());
					String mentioner = Bytes.toString(kv.getQualifier());
					if(map.containsKey(mentioner)) {
						value += map.get(mentioner);
					}
					map.put(mentioner, value);
				}
			}
			
			for(Map.Entry<String, Integer> e : map.entrySet()) {
				if(e.getValue()>= minTimes) {
					int value = 1;
					String key = e.getKey();
					if(general.containsKey(key)) {
						value += general.get(key);
					}
					general.put(key, value);
				}
			}
		}
		
		
		List<Author> list = new ArrayList<Author>();
		for(Map.Entry<String, Integer> el : general.entrySet()) {
			int counter = el.getValue();
			if(counter >= minMentionedAuthors) {
				list.add(new Author(Long.parseLong(el.getKey()),counter));
			}
		}
		this.getQuery().updateUsers(list);
	}*/
	
	@Override
	public void execute(final Authors authors) throws IOException {
		
		final int minMentionedAuthors = this.getAtLeast().getLowerBound();
		final int minTimes = this.getAtLeastTimes().getTimes();
		byte[][] auths = new byte[authors.size()][];
		int i = 0;
		long min = Long.MAX_VALUE;
		long max = 0;
		for(Author a : authors.getAuthors()) {
			auths[i] = Bytes.toBytes(String.valueOf(a.getId()));
			i++;
		}
		
		
		Map<Long,List<String>> toPass = new HashMap<Long,List<String>>();
		for(Mention m : this.getMentions()) {
			List<String> lista = new ArrayList<String>();
			long id = m.getMentioned().getId();
			lista.add(this.timeRange.generateFirstRowKey(id));
			lista.add(this.timeRange.generateLastRowKey(id));
			if(id < min)
				min = id;
			if(id > max)
				max = id;
		}
		
		String minRowKey = this.timeRange.generateFirstRowKey(min);
		String maxRowKey = this.timeRange.generateLastRowKey(max);
		
		Batch.Call call = null;
		try {
			if(this.timeRange instanceof ThisYear) {
				call = Batch.forMethod(AuthorAggregatorProtocol.class,
										"aggregateMentionsByMMThisYear", auths, toPass);
			} else {
				call = Batch.forMethod(AuthorAggregatorProtocol.class,
						"aggregateMentionsByMMThisYear", auths, toPass);
			}
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
		
		Map<byte[], Map<String, Map<String, Integer>>> results = null;
		try {
			results = (Map<byte[], Map<String,Map<String,Integer>>>) this.client.coprocessorExec(AuthorAggregatorProtocol.class,
					Bytes.toBytes(minRowKey), Bytes.toBytes(maxRowKey), call);
		} catch (Throwable e1) {
			e1.printStackTrace();
		}
		
		Map<String,Map<String,Integer>> firstAggregation = new HashMap<String,Map<String,Integer>> ();
		for (Map.Entry<byte[], Map<String,Map<String,Integer>>> entry : results.entrySet()) {
			for(Map.Entry<String,Map<String,Integer>> res : entry.getValue().entrySet()) {
				String mentioned = res.getKey();
				Map<String,Integer> subMap;
				if(firstAggregation.containsKey(mentioned)){
					subMap = firstAggregation.get(mentioned);
				}
				else {
					subMap = new HashMap<String,Integer>();
				}
				for(Map.Entry<String,Integer> singleCount : res.getValue().entrySet()){
					int value = singleCount.getValue();
					String mentioner = singleCount.getKey();
					if(subMap.containsKey(mentioner)){
						value += subMap.get(mentioner);
					}
					subMap.put(mentioner, value);
				}
				firstAggregation.put(mentioned, subMap);
			}
		}
		
		/* firstAggregation contains the global data, now get the one with value >= minTimes */
		Map<String,Integer> mentionerMap = new HashMap<String,Integer>();
		for(Map.Entry<String, Map<String,Integer>> e : firstAggregation.entrySet()) {
			for(Map.Entry<String,Integer> f : e.getValue().entrySet()) {
				if(f.getValue() >= minTimes) {
					String mentioner = f.getKey();
					int value = 1;
					if(mentionerMap.containsKey(mentioner)) {
						value += mentionerMap.get(mentioner);
					}
					mentionerMap.put(mentioner, value);
				}
			}
		}
				
		List<Author> list = new ArrayList<Author>();
		for(Map.Entry<String, Integer> el : mentionerMap.entrySet()) {
			int counter = el.getValue();
			if(counter >= minMentionedAuthors) {
				list.add(new Author(Long.parseLong(el.getKey()),counter));
			}
		}
		this.getQuery().updateUsers(list);
	}
}
