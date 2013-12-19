package hbase.impls;

import java.io.IOException;

import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Query exector. As far as the system has been developed, it doesn't do much,
 * but it could be improved as new needs arise.
 * @author Daniele Morgantini
 * */
public class HQueryManager {
	
	public HQueryManager() {
	}
	
	public Authors answer(final HQuery q) throws IOException {
		return q.answer();
	}

}
