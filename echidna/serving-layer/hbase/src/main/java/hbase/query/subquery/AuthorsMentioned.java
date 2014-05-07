package hbase.query.subquery;

import java.util.ArrayList;
import java.util.List;

import hbase.query.AtLeastTimes;
import hbase.query.HQuery;
import hbase.query.Mention;

/**
 * Subquery to represent the authors-mentioned request
 * @author Daniele Morgantini
 */
public abstract class AuthorsMentioned extends HSubQuery {
				
	private AtLeastTimes times;
	
	private List<Mention> mentions;
	
	/**
	 * Creates an instance of AuthorsMentioned subquery
	 * @return an instance of AuthorsMentioned subquery
	 * @param query the belonging query
	 * @param times the minimum number of mentions per mentioned authors
	 * @param mentions the mentions of authors
	 */
	public AuthorsMentioned(final HQuery query, final AtLeastTimes times, final Mention...mentions) {
		super(query);
		this.times = times;
		this.mentions = new ArrayList<Mention>();
		for(Mention m : mentions)
			this.mentions.add(m);
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

	/**
	 * Retrieves the mentions
	 * @return the mentions
	 */
	public List<Mention> getMentions() {
		return mentions;
	}

	/**
	 * Sets the mentions
	 * @param the mentions to set
	 */
	public void setMentions(List<Mention> mentions) {
		this.mentions = mentions;
	}

}
