package hbase.query.subquery;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Simple class to represent a composed subquery
 * @author Daniele Morgantini
 */
public class HSubQueryComposed extends HSubQuery{
	
	private HQuery query;
	
	private Queue<HSubQueryPart> subqueryParts;
	
	private Authors partialResult;
	
	/**
	 * Creates an instance of HSubQuery and attaches it to the belonging HQuery
	 * @return a HSubQuery instance
	 * @param query the query the subquery belongs to
	 */
	public HSubQueryComposed(final HQuery query) {
		super(query);
		this.subqueryParts = new LinkedList<HSubQueryPart>();
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
	 * Adds a subquery part to the components of the subquery
	 * @param part the subquery part to add
	 */
	public void addSubqueryPart(HSubQueryPart part) {
		this.subqueryParts.add(part);
	}
	
	/**
	 * Execute the subquery
	 * @param authors the authors against who execute the subquery
	 */
	public void execute(final Authors authors) throws IOException {
		this.partialResult = authors;
		while(!this.subqueryParts.isEmpty()){
			HSubQueryPart part = this.subqueryParts.poll();
			this.partialResult = part.execute(this.partialResult);
		}
		this.query.updateUsers(this.partialResult.getAuthors());
	};

}
