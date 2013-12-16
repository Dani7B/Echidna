package hbase.impls;

import java.io.IOException;

import hbase.query.Authors;
import hbase.query.HQuery;

public class HQueryManager {

				
	public HQueryManager() {
	}
	
	/*
	HQuery query = new HQuery()
		.users()
		.thatMentioned(new LastMonth(), new AtLeast(1), new Mention(1), new Mention(2))
		.whoFollow(new AtLeast(1), new Author(10), new Author(20))
		.rankedById(true)
		.take(2);*/
	public Authors answer(HQuery q) throws IOException {
		return q.answer();
	}

}
