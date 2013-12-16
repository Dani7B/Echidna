package hbase.query.subquery;

import java.io.IOException;

import hbase.query.Authors;
import hbase.query.HQuery;

public abstract class HSubQuery {
	
	private HQuery query;
	
	public HSubQuery() {
		
	}
	
	public HSubQuery(HQuery query) {
		this.query = query;
		this.query.addSubquery(this);
	}

	public HQuery getQuery() {
		return query;
	}

	public void setQuery(HQuery query) {
		this.query = query;
	}
	
	public abstract void execute(Authors authors) throws IOException;

}
