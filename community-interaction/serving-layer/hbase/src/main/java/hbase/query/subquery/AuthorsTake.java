package hbase.query.subquery;

import java.util.ArrayList;
import java.util.List;

import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

/**
 * Subquery to represent the authors-take request
 * @author Daniele Morgantini
 */
public class AuthorsTake extends HSubQuery {
	
	private int amount;
	
	
	/**
	 * Creates an instance of AuthorsTake subquery
	 * @return an instance of AuthorsTake subquery
	 * @param query the belonging query
	 * @param amount the number of authors to retrieve
	 */
	public AuthorsTake(HQuery query, int amount) {
		super(query);
		this.amount = amount;
	}

	@Override
	public void execute(Authors authors) {
		
		List<Author> result = new ArrayList<Author>();
		int i = 0;
		for(Author a : authors.getAuthors()){
			if(i < this.amount) {
				result.add(a);
				i++;
			}
			else
				break;
		}
		this.getQuery().updateUsers(result);
	}

}
