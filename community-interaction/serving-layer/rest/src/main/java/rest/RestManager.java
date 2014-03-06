package rest;

import java.util.ArrayList;
import java.util.List;

import hbase.query.AtLeast;
import hbase.query.AtLeastTimes;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.LastMonth;
import hbase.query.time.LastMonthFromNow;
import hbase.query.time.MonthsAgo;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
 
@Path("/search")
public class RestManager {
	
	private Authors authors;
	
	private HQuery query;
 
	public RestManager() {
		this.query = new HQuery();
		this.authors = new Authors(query);
	}
	
	@GET
	@Path("/users")
	public Response generic(
		@DefaultValue("1") @QueryParam("tm_atLeast") int tm_al,
		@QueryParam("tm_when") String tm_when,
		@QueryParam("tm_users") String tm_users,
		@DefaultValue("1") @QueryParam("wf_atLeast") int wf_al,
		@QueryParam("wf_users") String wf_users,
		@QueryParam("wff_users") String wff_users,
		@QueryParam("wfafb_users") String wfafb_users,
		@DefaultValue("1") @QueryParam("wfm_atLeast") int wfm_al,
		@QueryParam("wfm_when") String wfm_when,
		@QueryParam("wfm_back") int wfm_back,
		@DefaultValue("1") @QueryParam("wfm_minTimes") int wfm_minTimes,
		@QueryParam("wfm_users") String wfm_users,
		@QueryParam("byId") String byId,
		@DefaultValue("true") @QueryParam("byHits") boolean byHits,
		@DefaultValue("10") @QueryParam("take") int take){
		
		String output = "Users";
		if(tm_users!=null && tm_when!=null){
			this.authorsThatMentionedSubquery(tm_al, tm_when, tm_users);
			output += " that mentioned " +
					"at least: " + tm_al + " user";
			if(tm_al>1)
				output += "s";
			output += " when: " + tm_when +
					" amongst: " + tm_users + ",";
		}
		
		if(wf_users!=null){
			this.authorsWhoFollowSubquery(wf_al, wf_users);
			output += " who follow " +
					"at least: " + wf_al + " user";
			if(wf_al>1)
				output += "s";
			output += " amongst: " + wf_users + ",";
		}
		
		if(wff_users!=null) {
			this.authorsWhoseFollowersFollowSubquery(wff_users);
			output += " whose followers follow one amongst " + wff_users + ",";
		}
		
		if(wfafb_users!=null) {
			this.authorsWhoseFollowersAreFollowedBySubquery(wfafb_users);
			output += " whose followers are followed by one amongst " + wfafb_users + ",";
		}
		
		if(wfm_users!=null) {
			this.authorsWhoseFollowersMentionedSubquery(wfm_al, wfm_when, wfm_back, wfm_users, wfm_minTimes);
			output += " whose followers mentioned" + 
						" at least: " + wfm_al + " user";
			if(wfm_al>1)
				output += "s";
			output += " when: " + wfm_when;
			if(wfm_back>0)
				output += " (" + wfm_back + ")";
			output += " amongst: " + wfm_users +
						" exactly or more than " + wfm_minTimes + " time";
			if(wfm_minTimes>1)
				output += "s";
			output += ",";
		}
		
		if(byId!=null) {
			boolean b = Boolean.parseBoolean(byId);
			this.authors.rankedById(b);
		}
		else {
			this.authors.rankedByHits(byHits);
		}
		this.authors.take(take);
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String qAndA = output.substring(0, output.length()-1);
		qAndA += " ----> " + result;
		return Response.status(200).entity(qAndA).build();
	}
	
	@GET
	@Path("/users/thatMentioned")
	public Response usersThatMentioned(
		@DefaultValue("1") @QueryParam("atLeast") int tm_al,
		@QueryParam("when") String tm_when,
		@QueryParam("users") String tm_users,
		@DefaultValue("true") @QueryParam("byId") boolean byId,
		@DefaultValue("10") @QueryParam("take") int take){
		
		this.authorsThatMentionedSubquery(tm_al, tm_when, tm_users);
		this.authors.rankedById(byId);
		if(take>0) {
			this.authors.take(take);
		}
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users that mentioned" +
						" at least: " + tm_al + " user";
		if(tm_al>1)
			output += "s";
		output += " when: " + tm_when +
				" amongst: " + tm_users +
				" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/whoFollow")
	public Response usersWhoFollow(
		@DefaultValue("1") @QueryParam("atLeast") int wf_al,
		@QueryParam("users") String wf_users,
		@DefaultValue("true") @QueryParam("byId") boolean byId,
		@DefaultValue("10") @QueryParam("take") int take){
			
		this.authorsWhoFollowSubquery(wf_al, wf_users);
		this.authors.rankedById(byId);
		if(take>0) {
			this.authors.take(take);
		}
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users who follow" +
						" at least: " + wf_al + " user";
		if(wf_al>1)
			output += "s";
		output += " amongst: " + wf_users +
				  " ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/whoseFollowersFollow")
	public Response usersWhoseFollowersFollow(
		@QueryParam("users") String users,
		@DefaultValue("true") @QueryParam("byHits") boolean byHits,
		@DefaultValue("10") @QueryParam("take") int take){
			
		this.authorsWhoseFollowersFollowSubquery(users);
		this.authors.rankedByHits(byHits);
		if(take>0) {
			this.authors.take(take);
		}
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users whose followers follow one amongst " + users +
		" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/whoseFollowersAreFollowedBy")
	public Response usersWhoseFollowersAreFollowedBy(
		@QueryParam("users") String users,
		@DefaultValue("true") @QueryParam("byHits") boolean byHits,
		@DefaultValue("10") @QueryParam("take") int take){
			
		this.authorsWhoseFollowersAreFollowedBySubquery(users);
		this.authors.rankedByHits(byHits);
		if(take>0) {
			this.authors.take(take);
		}
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users whose followers are followed by one amongst" + users +
		" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/whoseFollowersMentioned")
	public Response usersWhoseFollowersMentioned(
		@QueryParam("when") String when,
		@QueryParam("back") int back,
		@DefaultValue("1") @QueryParam("atLeast") int al,
		@DefaultValue("1") @QueryParam("minTimes") int minTimes,
		@QueryParam("users") String users,
		@DefaultValue("true") @QueryParam("byHits") boolean byHits,
		@DefaultValue("10") @QueryParam("take") int take){
			
		this.authorsWhoseFollowersMentionedSubquery(al, when, back, users, minTimes);
		this.authors.rankedByHits(byHits);
		if(take>0) {
			this.authors.take(take);
		}
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users whose followers mentioned" + 
						" at least: " + al + " user";
		if(al>1)
			output += "s";
		output += " when: " + when;
		if(back>0)
			output += " (" + back + ")";
		output += " exactly or more than " + minTimes + " time";
		if(minTimes>1)
			output += "s";
		output += " amongst: " + users +
				  " ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	private void authorsThatMentionedSubquery(int atLeast, String when, String users) {
		Mention[] mentions = this.mentionsFromString(users);
		switch(when) {
			case "last_month":
				this.authors.thatMentioned(new LastMonth(), new AtLeast(atLeast), mentions);
				break;
			case "very_last_month":
				this.authors.thatMentioned(new LastMonthFromNow(), new AtLeast(atLeast), mentions);
				break;
		}
	}
	
	private void authorsWhoFollowSubquery(int atLeast, String users) {
		Author[] auths = authorsFromString(users);
		this.authors.whoFollow(new AtLeast(atLeast), auths);
	}
	
	private void authorsWhoseFollowersFollowSubquery(String users) {
		Author[] followed = authorsFromString(users);
		this.authors.whoseFollowersFollow(followed);
	}
	
	private void authorsWhoseFollowersAreFollowedBySubquery(String users) {
		Author[] followers = authorsFromString(users);
		this.authors.whoseFollowersAreFollowedBy(followers);
	}
	
	private void authorsWhoseFollowersMentionedSubquery(int atLeast, String when, 
										int back, String users, int minTimes) {
		
		Mention[] mentions = mentionsFromString(users);
		switch(when){
			case "last_month":
				this.authors.whoseFollowersMentioned(new LastMonth(), new AtLeast(atLeast),
						new AtLeastTimes(minTimes), mentions);
				break;
			case "months_ago":
				this.authors.whoseFollowersMentioned(new MonthsAgo(back), new AtLeast(atLeast),
						new AtLeastTimes(minTimes), mentions);
				break;
		}
	}
	
	private Author[] authorsFromString(String users) {
		String[] splitted = users.split(",");
		Author[] authors = new Author[splitted.length];
		for(int i=0; i<splitted.length; i++) {
			authors[i] = new Author(Long.parseLong(splitted[i]));
		}
		return authors;
	}
	
	private Mention[] mentionsFromString(String users) {
		String[] splitted = users.split(",");
		Mention[] mentions = new Mention[splitted.length];
		for(int i=0; i<splitted.length; i++) {
			mentions[i] = new Mention(Long.parseLong(splitted[i]));
		}
		return mentions;
	}
 
}