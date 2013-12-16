package hbase.query.subquery;

import java.util.ArrayList;
import java.util.List;

import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;

public class AuthorsTake extends HSubQuery {
	
	private int amount;
		
	public AuthorsTake(HQuery query, int amount) {
		super(query);
		this.amount = amount;
	}

	@Override
	public void execute(Authors authors) {
		
		List<Author> result = new ArrayList<Author>();
		int i = 0;
		for(Author a : authors.getAuthors()){
			if(i <= this.amount) {
				result.add(a);
			}
			else
				break;
		}
		authors.setAuthors(result);
	}

}
