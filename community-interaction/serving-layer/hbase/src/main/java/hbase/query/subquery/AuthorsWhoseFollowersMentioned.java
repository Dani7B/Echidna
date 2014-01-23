package hbase.query.subquery;

import hbase.query.AtLeast;
import hbase.query.AtLeastTimes;
import hbase.query.HQuery;
import hbase.query.Mention;

/**
 * Subquery to represent the authors-whose-followers-mentioned request
 * @author Daniele Morgantini
 */
public abstract class AuthorsWhoseFollowersMentioned extends AuthorsThatMentioned {
				
	private AtLeastTimes times;
			
	
	/**
	 * Creates an instance of AuthorsWhoseFollowersMentioned subquery
	 * @return an instance of AuthorsWhoseFollowersMentioned subquery
	 * @param query the belonging query
	 * @param atLeast the minimum number of authors to mention
	 * @param times the minimum number of mentions for each mentioned authors
	 * @param mentions the mentions of authors
	 */
	public AuthorsWhoseFollowersMentioned(final HQuery query, final AtLeast atLeast, 
											final AtLeastTimes times, final Mention...mentions) {
		super(query, atLeast, mentions);
		this.times = times;
	}


	/**
	 * Retrieves the minimum allowed number of mentions
	 * @return the minimum allowed number of mentions
	 */
	public AtLeastTimes getAtLeastTimes() {
		return times;
	}

	/**
	 * Sets the minimum allowed number of mentions
	 * @param the minimum allowed number of mentions to set
	 */
	public void setAtLeastTimes(AtLeastTimes times) {
		this.times = times;
	}

}
