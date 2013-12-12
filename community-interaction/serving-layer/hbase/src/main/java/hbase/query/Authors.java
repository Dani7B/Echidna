package hbase.query;

import hbase.query.time.TimeRange;

import java.util.ArrayList;
import java.util.List;

public class Authors {
	
	private HQuery query;
	
	private TimeRange range;
	
	private AtLeast mentionAtLeast;
	
	private AtLeast followAtLeast;
	
	private List<Mention> mentions;
	
	private List<Author> followed;

	private boolean idRanked;
	
	private List<Author> authors;

	
	public Authors(HQuery q) {
		this.query = q;
	}
	
	public Authors(List<Author> authors) {
		this.authors = authors;
	}
	
	
	public Authors thatMentioned(TimeRange range, AtLeast atLeast, Mention... mentions) {
		this.range = range;
		this.mentionAtLeast = atLeast;
		this.mentions = new ArrayList<Mention>();
		for(Mention m : mentions)
			this.mentions.add(m);
        return this;
    }
	
	
	public Authors whoFollow(AtLeast atLeast, Author... authors) {
		this.followAtLeast = atLeast;
		this.followed = new ArrayList<Author>();
		for(Author a : authors)
			this.followed.add(a);
		return this;
    }
	
	public Authors rankedById() {
		this.idRanked = true;
        return this;
    }

	
	public HQuery take(int amount) {
        return this.query.take(amount);
    }


	public TimeRange getTimeRange() {
		return this.range;
	}


	public AtLeast getMentionAtLeast() {
		return this.mentionAtLeast;
	}

	public AtLeast getFollowAtLeast() {
		return this.followAtLeast;
	}

	public List<Mention> getMentions() {
		return this.mentions;
	}
	
	public List<Author> getFollowed() {
		return this.followed;
	}

	public List<Author> getAuthors() {
		return authors;
	}

	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}

	public boolean isIdRanked() {
		return idRanked;
	}

	public void setIdRanked(boolean idRanked) {
		this.idRanked = idRanked;
	}

}
