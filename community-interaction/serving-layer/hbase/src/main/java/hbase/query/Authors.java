package hbase.query;

import hbase.HBaseClient;
import hbase.impls.HBaseClientFactory;
import hbase.query.subquery.AuthorsRankedById;
import hbase.query.subquery.AuthorsTake;
import hbase.query.subquery.AuthorsThatMentioned;
import hbase.query.subquery.AuthorsWhoFollow;
import hbase.query.subquery.HSubQuery;
import hbase.query.time.TimeRange;

import java.util.ArrayList;
import java.util.List;


/**
 * Simple class to handle a collection of Authors and queries associated to it
 * @author Daniele Morgantini
 */
public class Authors {
	
	private HQuery query;
	
	private List<Author> authors;

	
	/**
	 * Create an instance of Authors
	 * @param query the query the author collection is associated to
	 */
	public Authors(HQuery query) {
		this.query = query;
		this.authors = new ArrayList<Author>();
	}
	
	/**
	 * Adds the authors-that-mentioned subquery to the query
	 * @return this
	 * @param range the time range to take into account
	 * @param atLeast the minimum number of mentions per author
	 * @param mentions the mentions
	 */
	public Authors thatMentioned(TimeRange timeRange, AtLeast atLeast, Mention... mentions) {
		HBaseClient client = HBaseClientFactory.getInstance().getMentionedBy();
    	HSubQuery sub = new AuthorsThatMentioned(this.query, client, timeRange, atLeast, mentions);
        return this;
    }
	
	/**
	 * Adds the authors-who-follow subquery to the query
	 * @return this
	 * @param atLeast the minimum number of mentions per author
	 * @param followed the followed authors
	 */
	public Authors whoFollow(AtLeast atLeast, Author... followed) {
		HBaseClient client = HBaseClientFactory.getInstance().getFollowedBy();
    	HSubQuery sub = new AuthorsWhoFollow(this.query, client, atLeast, followed);
		return this;
    }
	
	
	/**
	 * Adds the authors-ranked-by-id subquery to the query
	 * @return this
	 * @param ascOrDesc the kind of order desired (true=ascendent, false=descendent)
	 */
	public Authors rankedById(boolean ascOrDesc) {
    	HSubQuery sub = new AuthorsRankedById(this.query, ascOrDesc);
        return this;
    }

	/**
	 * Adds the authors-take subquery to the query
	 * @return the query
	 * @param amount the number of authors to return
	 */
	public HQuery take(int amount) {
		HSubQuery sub = new AuthorsTake(this.query, amount);
        return this.query;
    }

	/**
	 * Retrieves the authors list
	 * @return the authors list
	 */
	public List<Author> getAuthors() {
		return authors;
	}

	/**
	 * Sets the authors list
	 * @param the authors list
	 */
	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}
	
	/**
	 * Retrieve the string representation of the authors list
	 * @return the string representation of the authors list
	 */
	public String toString() {
		String result = "[ ";
		for(Author a : this.authors){
			result += a.getId() + " ";
		}
		result += "]";
		return result;
	}

}