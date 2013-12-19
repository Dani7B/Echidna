package hbase.query.subquery;

import java.io.IOException;

import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Simple abstract class to represent a subquery
 * @author Daniele Morgantini
 */
public abstract class HSubQuery {
	
	private HQuery query;
	
	/**
	 * Creates an instance of HSubQuery and attaches it to the belonging HQuery
	 * @return a HSubQuery instance
	 * @param query the query the subquery belongs to
	 */
	public HSubQuery(final HQuery query) {
		this.query = query;
		this.query.addSubquery(this);
	}

	/**
	 * Retrieves the belonging query
	 * @return the belonging query
	 */
	public HQuery getQuery() {
		return query;
	}

	/**
	 * Sets the belonging query
	 * @param query the belonging query
	 */
	public void setQuery(final HQuery query) {
		this.query = query;
	}
	
	/**
	 * Execute the subquery
	 * @param authors the authors against who execute the query
	 */
	public abstract void execute(final Authors authors) throws IOException;

}
