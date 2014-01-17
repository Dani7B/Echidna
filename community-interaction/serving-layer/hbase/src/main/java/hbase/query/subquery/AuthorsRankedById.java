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
public class AuthorsRankedById extends HSubQuery {

	boolean asc;
	
	/** 
	 * Creates an instance of AuthorsRankedById subquery
	 * @return an instance of AuthorsRankedById subquery
	 * @param query the belonging query
	 * @param ascOrDesc the desired order (ascendent=true, descendent=false)
	 */
	public AuthorsRankedById(final HQuery query, final boolean ascOrDesc) {
		super(query);
		this.asc = ascOrDesc;
	}

	@Override
	public void execute(final Authors authors) {
		
		if(asc) {
			Collections.sort(authors.getAuthors(), new AuthorComparerAsc());
		}
		else {
			Collections.sort(authors.getAuthors(), new AuthorComparerDesc());
		}
		this.getQuery().updateUsers(authors.getAuthors());

	}

	
	/**
	 * Private class to perform the sort by ascending order
	 * @author Daniele Morgantini
	 */
	private static class AuthorComparerAsc implements Comparator<Author> {
		  
		@Override
		  public int compare(final Author x, final Author y) {
			return compare(x.getId(), y.getId());
		  }

		  private static int compare(final long a, final long b) {
		    return a < b ? -1
		         : a > b ? 1
		         : 0;
		  }
	}
	
	/**
	 * Private class to perform the sort by descending order
	 * @author Daniele Morgantini
	 */
	private static class AuthorComparerDesc implements Comparator<Author> {
		  
		@Override
		  public int compare(final Author x, final Author y) {
			return compare(x.getId(), y.getId());
		  }

		  private static int compare(final long a, final long b) {
		    return a < b ? 1
		         : a > b ? -1
		         : 0;
		  }
	}
}
