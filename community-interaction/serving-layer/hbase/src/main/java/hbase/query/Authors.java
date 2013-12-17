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

public class Authors {
	
	private HQuery query;
	
	private List<Author> authors;

	
	public Authors(HQuery q) {
		this.query = q;
		this.authors = new ArrayList<Author>();
	}
	
	public Authors thatMentioned(TimeRange range, AtLeast atLeast, Mention... mentions) {
		HBaseClient client = HBaseClientFactory.getInstance().getMentionedBy();
    	HSubQuery sub = new AuthorsThatMentioned(this.query, client, range, atLeast, mentions);
        return this;
    }
	
	
	public Authors whoFollow(AtLeast atLeast, Author... followed) {
		HBaseClient client = HBaseClientFactory.getInstance().getFollowedBy();
    	HSubQuery sub = new AuthorsWhoFollow(this.query, client, atLeast, followed);
		return this;
    }
	
	public Authors rankedById(boolean ascOrDesc) {
		
    	HSubQuery sub = new AuthorsRankedById(this.query, ascOrDesc);
        return this;
    }

	
	public HQuery take(int amount) {
		HSubQuery sub = new AuthorsTake(this.query, amount);
        return this.query;
    }


	public List<Author> getAuthors() {
		return authors;
	}

	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}
	
	public String toString() {
		String result = "[ ";
		for(Author a : this.authors){
			result += a.getId() + " ";
		}
		result += "]";
		return result;
	}

}
