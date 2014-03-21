package hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hbase.query.subquery.HSubQuery;

/**
 * Simple class to represent a query
 * @author Daniele Morgantini
 */
public class HQuery {
	
	private Authors users;
	
	private List<HSubQuery> subqueries;
	
	/**
	 * No argument consturctor
	 * @return an HQuery instance
	 */
	public HQuery() {
		this.users = new Authors(this);
		this.subqueries = new ArrayList<HSubQuery>();
	}
	
	/**
	 * Retrieves the authors associated to the query
	 * @return the authors associated to the query 
	 */
	public Authors users() {
		return this.users;
	}
	
	/**
	 * Updates the authors associated to the query
	 * @param authors the list of authors to set
	 */
	public void updateUsers(final List<Author> authors) {
		this.users.setAuthors(authors);;
	}
	
	
	/**
	 * Retrieves the subqueries the query is composed of
	 * @return the list of subqueries the query is composed of
	 */
	public List<HSubQuery> getSubqueries() {
		return this.subqueries;
	}
	
	
	/**
	 * Adds a subquery to the subqueries the query is composed of
	 * @param subquery the subquery to add
	 */
	public void addSubquery(final HSubQuery subquery) {
		this.subqueries.add(subquery);
	}
	
	
	/**
	 * Answers the query
	 * @return the authors that match the query
	 * */
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
