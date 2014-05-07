package hbase.query.subquery;

import java.util.Collections;
import java.util.Comparator;

import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Subquery to represent the ordering of authors by id
 * @author Daniele Morgantini
 */
public class AuthorsRankedByHits extends HSubQuery {

	boolean asc;
	
	/** 
	 * Creates an instance of AuthorsRankedByHits subquery part
	 * @return an instance of AuthorsRankedByHits subquery part
	 * @param query the belonging query
	 * @param ascOrDesc the desired order (ascendent=true, descendent=false)
	 */
	public AuthorsRankedByHits(final HQuery query, final boolean ascOrDesc) {
		super(query);
		this.asc = ascOrDesc;
	}

	@Override
	public void execute(final Authors authors) {
		
		if(asc) {
			Collections.sort(authors.getAuthors(), new AuthorHitsComparerAsc());
		}
		else {
			Collections.sort(authors.getAuthors(), new AuthorHitsComparerDesc());
		}
		this.getQuery().updateUsers(authors.getAuthors());

	}

	
	/**
	 * Private class to perform the sort by ascending order
	 * @author Daniele Morgantini
	 */
	private static class AuthorHitsComparerAsc implements Comparator<Author> {
		  
		@Override
		  public int compare(final Author x, final Author y) {
			return compare(x.getHits(), y.getHits());
		  }

		  private static int compare(final long a, final long b) {
		    return b < a ? -1
		         : b > a ? 1
		         : 0;
		  }
	}
	
	/**
	 * Private class to perform the sort by descending order
	 * @author Daniele Morgantini
	 */
	private static class AuthorHitsComparerDesc implements Comparator<Author> {
		  
		@Override
		  public int compare(final Author x, final Author y) {
			return compare(x.getHits(), y.getHits());
		  }

		  private static int compare(final long a, final long b) {
		    return b < a ? 1
		         : b > a ? -1
		         : 0;
		  }
	}
}
