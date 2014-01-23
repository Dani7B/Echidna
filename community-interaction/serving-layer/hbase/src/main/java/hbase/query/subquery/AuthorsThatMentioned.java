package hbase.query.subquery;

import java.util.ArrayList;
import java.util.List;

import hbase.query.AtLeast;
import hbase.query.HQuery;
import hbase.query.Mention;

/**
 * Subquery to represent the authors-that-mentioned request
 * @author Daniele Morgantini
 */
public abstract class AuthorsThatMentioned extends HSubQuery {
			
	private AtLeast atLeast;
	
	private List<Mention> mentions;
		
	
	/**
	 * Creates an instance of AuthorsThatMentioned subquery
	 * @return an instance of AuthorsThatMentioned subquery
	 * @param query the belonging query
	 * @param atLeast the minimum number of authors to mention
	 * @param mentions the mentions of authors
	 */
	public AuthorsThatMentioned(final HQuery query, final AtLeast atLeast, final Mention...mentions) {
		super(query);
		this.atLeast = atLeast;
		this.mentions = new ArrayList<Mention>();
		for(Mention m : mentions)
			this.mentions.add(m);
	}

	/**
	 * Retrieves the minimum allowed number of mentioned authors
	 * @return the minimum allowed number of mentioned authors
	 */
	public AtLeast getAtLeast() {
		return atLeast;
	}

	/**
	 * Sets the minimum allowed number of mentioned authors
	 * @param the minimum allowed number of mentioned authors to set
	 */
	public void setAtLeast(AtLeast atLeast) {
		this.atLeast = atLeast;
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
