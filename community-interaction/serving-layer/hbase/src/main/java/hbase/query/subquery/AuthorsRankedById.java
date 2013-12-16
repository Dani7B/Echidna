package hbase.query.subquery;

import java.util.Collections;
import java.util.Comparator;

import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

public class AuthorsRankedById extends HSubQuery {

	boolean asc;
	
	public AuthorsRankedById(HQuery query, boolean ascOrDesc) {
		super(query);
		this.asc = ascOrDesc;
	}

	@Override
	public void execute(Authors authors) {
		
		if(asc) {
			Collections.sort(authors.getAuthors(), new AuthorComparerAsc());
		}
		else {
			Collections.sort(authors.getAuthors(), new AuthorComparerDesc());
		}
	}

	private static class AuthorComparerAsc implements Comparator<Author> {
		  
		@Override
		  public int compare(Author x, Author y) {
			return compare(x.getId(), y.getId());
		  }

		  private static int compare(long a, long b) {
		    return a < b ? -1
		         : a > b ? 1
		         : 0;
		  }
	}
	
	private static class AuthorComparerDesc implements Comparator<Author> {
		  
		@Override
		  public int compare(Author x, Author y) {
			return compare(x.getId(), y.getId());
		  }

		  private static int compare(long a, long b) {
		    return a < b ? 1
		         : a > b ? -1
		         : 0;
		  }
	}
}
