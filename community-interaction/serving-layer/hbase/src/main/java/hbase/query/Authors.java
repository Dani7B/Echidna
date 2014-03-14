package hbase.query;

import hbase.query.subquery.AuthorsRankedByHits;
import hbase.query.subquery.AuthorsRankedById;
import hbase.query.subquery.AuthorsTake;
import hbase.query.subquery.AuthorsThatMentionedBackwards;
import hbase.query.subquery.AuthorsThatMentionedFixedTime;
import hbase.query.subquery.AuthorsWhoFollow;
import hbase.query.subquery.AuthorsWhoseFollowersAreFollowedBy;
import hbase.query.subquery.AuthorsWhoseFollowersFollow;
import hbase.query.subquery.AuthorsWhoseFollowersMentionedFixedTime;
import hbase.query.subquery.HSubQuery;
import hbase.query.time.FixedTime;
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
	public Authors(final HQuery query) {
		this.query = query;
		this.authors = new ArrayList<Author>();
	}
	
	/**
	 * Checks if the authors list is empty
	 * @return true if the authors list is empty, false otherwise
	 * */
	public boolean isEmpty() {
		return this.authors.isEmpty();
	}
	
	
	/**
	 * Retrieves the number of authors
	 * @return the number of authors
	 * */
	public int size() {
		return this.authors.size();
	}
	
	/**
	 * Adds the authors-that-mentioned subquery to the query
	 * @return this
	 * @param timeRange the specific time range to take into account
	 * @param atLeast the minimum number of mentioned authors
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions
	 */
	public Authors thatMentioned(final TimeRange timeRange, final AtLeast atLeast, final Mention... mentions) {
    	HSubQuery sub = new AuthorsThatMentionedBackwards(this.query, timeRange, atLeast, mentions);
        return this;
    }
	
	/**
	 * Adds the authors-that-mentioned subquery to the query
	 * @return this
	 * @param timeRange the specific time range to take into account
	 * @param atLeast the minimum number of mentioned authors
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions
	 */
	public Authors thatMentioned(final TimeRange timeRange, final AtLeast atLeast,
									final AtLeastTimes times, final Mention... mentions) {
    	HSubQuery sub = new AuthorsThatMentionedBackwards(this.query, timeRange, atLeast, mentions);
        return this;
    }
	
	/**
	 * Adds the authors-that-mentioned subquery to the query
	 * @return this
	 * @param timeRange the fixed time range to take into account
	 * @param atLeast the minimum number of mentioned authors
	 * @param mentions the mentions
	 */
	public Authors thatMentioned(final FixedTime timeRange, final AtLeast atLeast, final Mention... mentions) {
    	HSubQuery sub = new AuthorsThatMentionedFixedTime(this.query, timeRange, atLeast, mentions);
        return this;
    }
	
	/**
	 * Adds the authors-that-mentioned subquery to the query
	 * @return this
	 * @param timeRange the fixed time range to take into account
	 * @param atLeast the minimum number of mentioned authors
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions
	 */
	public Authors thatMentioned(final FixedTime timeRange, final AtLeast atLeast,
									final AtLeastTimes times, final Mention... mentions) {
    	HSubQuery sub = new AuthorsThatMentionedFixedTime(this.query, timeRange, atLeast, times, mentions);
        return this;
    }
	
	/**
	 * Adds the authors-who-follow subquery to the query
	 * @return this
	 * @param atLeast the minimum number of mentions per author
	 * @param followed the followed authors
	 */
	public Authors whoFollow(final AtLeast atLeast, final Author... followed) {
    	HSubQuery sub = new AuthorsWhoFollow(this.query, atLeast, followed);
		return this;
    }
	
	
	/**
	 * Adds the authors-ranked-by-id subquery to the query
	 * @return this
	 * @param ascOrDesc the kind of order desired (true=ascendent, false=descendent)
	 */
	public Authors rankedById(final boolean ascOrDesc) {
    	HSubQuery sub = new AuthorsRankedById(this.query, ascOrDesc);
        return this;
    }

	/**
	 * Adds the authors-take subquery to the query
	 * @return the query
	 * @param amount the number of authors to return
	 */
	public HQuery take(final int amount) {
		HSubQuery sub = new AuthorsTake(this.query, amount);
        return this.query;
    }

	
	/**
	 * Adds the authors-whose-followers-follow subquery to the query
	 * @return this
	 * @param followed the followed authors
	 */
	public Authors whoseFollowersFollow(final Author... followed) {
    	HSubQuery sub = new AuthorsWhoseFollowersFollow(this.query, followed);
		return this;
    }
	
	/**
	 * Adds the authors-whose-followers-are-followed-by subquery to the query
	 * @return this
	 * @param followed the followed authors
	 */
	public Authors whoseFollowersAreFollowedBy(final Author... followers) {
    	HSubQuery sub = new AuthorsWhoseFollowersAreFollowedBy(this.query, followers);
		return this;
    }
	
	
	/**
	 * Adds the authors-whose-followers-mentioned subquery to the query
	 * @return this
	 * @param timeRange the specific time range to take into account
	 * @param atLeast the minimum number of mentioned authors
	 * @param times the minimum number of mentions per mentioned author
	 * @param mentions the mentions
	 */
	public Authors whoseFollowersMentioned(final FixedTime timeRange, final AtLeast atLeast, 
									final AtLeastTimes times, final Mention... mentions) {
    	HSubQuery sub = new AuthorsWhoseFollowersMentionedFixedTime(this.query, timeRange, atLeast, times, mentions);
        return this;
    }
	
	public Authors rankedByHits(final boolean ascOrDesc) {
    	HSubQuery sub = new AuthorsRankedByHits(this.query, ascOrDesc);
        return this;
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
	public void setAuthors(final List<Author> authors) {
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