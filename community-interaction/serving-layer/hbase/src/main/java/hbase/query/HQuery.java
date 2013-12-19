package hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hbase.query.subquery.HSubQuery;

public class HQuery {
	
	private Authors users;
	
	private List<HSubQuery> subqueries;
		
	public HQuery() {
		this.users = new Authors(this);
		this.subqueries = new ArrayList<HSubQuery>();
	}
	
	public Authors users() {
		return this.users;
	}
	
	public void updateUsers(final List<Author> authors) {
		this.users.setAuthors(authors);;
	}
	
	public List<HSubQuery> getSubqueries() {
		return this.subqueries;
	}
	
	public void addSubquery(final HSubQuery subquery) {
		this.subqueries.add(subquery);
	}
	
	
	public Authors answer() {
		
		for(HSubQuery s : this.subqueries)
			try {
				s.execute(this.users);
			} catch (IOException e) {
				e.printStackTrace();
			}
		return this.users;
	}

}
