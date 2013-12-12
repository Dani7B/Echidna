package hbase.impls;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.HBaseAdministrator;
import hbase.HBaseClient;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.Mention;
import hbase.query.HQuery;

public class HQueryManager {

	private HBaseClient mentionedBy;
	
	private HBaseClient followedBy;
	
	private HBaseAdministrator admin;
	
	public HQueryManager() throws IOException {
		
		this.admin = new HTableAdmin();
		this.mentionedBy = new HTableManager(admin.getTable("mentionedBy"));
		this.followedBy = new HTableManager(admin.getTable("followedBy"));
	}
	
	/*
	MyQuery query = new MyQuery()
		.users()
		.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(1), new Mention(2))
		.whoFollow(new AtLeast(1), new Author(10), new Author(20))
		.rankedById()
		.take(2);*/
	/* We know it's mentionersWhoFollow */
	public Authors answer(HQuery q) throws IOException { //Query
		Map<byte[],Integer> map = new HashMap<byte[],Integer>();
		Authors users = q.users();
		long lowerBound = users.getTimeRange().getStart();
		long upperBound = users.getTimeRange().getEnd();
		int mentionMin = users.getMentionAtLeast().getLowerBound();
		
		for(Mention m : users.getMentions()){
			
			SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");

			Date start = new Date(lowerBound);
			Date end = new Date(upperBound);
			String lowerRow = m.getMentioned().getId() + "_" + date.format(start);
			String upperRow = m.getMentioned().getId() + "_" + date.format(end);
			
			Result[] results = this.mentionedBy.scan(Bytes.toBytes(lowerRow),Bytes.toBytes(upperRow),
												Bytes.toBytes(lowerBound),Bytes.toBytes(upperBound));
			
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
		
		List<byte[]> partialResult = new ArrayList<byte[]>();
		for(Map.Entry<byte[], Integer> e : map.entrySet()) {
			int value = e.getValue();
			if(value >= mentionMin)
				partialResult.add(e.getKey());
		}
		
		byte[][] columns = new byte[partialResult.size()][];
		Map<Long,Integer> mapFollow = new HashMap<Long,Integer>();
		
		for(Author a : users.getFollowed()) {
			byte[] aID = Bytes.toBytes(a.getId());
			Result result = this.followedBy.get(aID, partialResult.toArray(columns));
			
			for(KeyValue kv : result.raw()) {
				long id = Bytes.toLong(kv.getQualifier());
				int value = 1;
				if(mapFollow.containsKey(id))
					value = mapFollow.get(id) + 1;
				mapFollow.put(id, value);
			}
		}
		
		List<Long> finalResult = new ArrayList<Long>();
		int followMin = users.getFollowAtLeast().getLowerBound();
		
		for(Map.Entry<Long, Integer> e : mapFollow.entrySet()) {
			if(e.getValue() >= followMin)
				finalResult.add(e.getKey());
		}
		
		if(users.isIdRanked()) {
			Collections.sort(finalResult);
		}
		
		List<Author> authors = new ArrayList<Author>();
		int i = 0;
		for(Long l : finalResult) {
			if(i<q.getAmount()){
				authors.add(new Author(l));
			}
			else
				break;
		}
		
		users.setAuthors(authors);
		return users;
		
	}

}
