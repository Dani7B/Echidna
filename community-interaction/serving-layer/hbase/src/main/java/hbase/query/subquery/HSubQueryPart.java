package hbase.query.subquery;

import java.io.IOException;

import hbase.HBaseClient;
import hbase.query.Authors;

/**
 * Simple abstract class to represent a subquery part
 * @author Daniele Morgantini
 */
public abstract class HSubQueryPart {
		
	private HSubQueryComposed subQuery;
	
	private HBaseClient client;
	
	/**
	 * Creates an instance of HSubQueryPart and attaches it to the belonging HSubQuery
	 * @return a HSubQueryPart instance
	 * @param subQuery the subquery the subquery part belongs to
	 */
	public HSubQueryPart(final HSubQueryComposed subQuery, final HBaseClient client) {
		this.subQuery = subQuery;
		this.subQuery.addSubqueryPart(this);
		this.client = client;
	}

	/**
	 * Retrieves the belonging subquery
	 * @return the belonging subquery
	 */
	public HSubQueryComposed getSubQuery() {
		return this.subQuery;
	}

	/**
	 * Sets the belonging subquery
	 * @param subquery the belonging subquery
	 */
	public void setSubQuery(final HSubQueryComposed subquery) {
		this.subQuery = subquery;
	}
	
	/**
	 * Retrieves the client associated to that subquery part
	 * @return the client associated to that subquery part
	 */
	public HBaseClient getClient() {
		return client;
	}

	/**
	 * Sets the client to perform the subquery part
	 * @param client the client to set
	 */
	public void setClient(HBaseClient client) {
		this.client = client;
	}

	/**
	 * Execute the subquery part
	 * @param authors the authors against who execute the subquery part
	 */
	public abstract Authors execute(final Authors authors) throws IOException;

}
