package rest;

import java.util.ArrayList;
import java.util.List;

import hbase.query.AtLeast;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.LastMonth;
import hbase.query.time.LastMonthFromNow;

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
	public Response getMsg(
		@QueryParam("tm_atLeast") int tm_al,
		@QueryParam("tm_when") String tm_when,
		@QueryParam("tm_users") String tm_users,
		@QueryParam("wf_atLeast") int wf_al,
		@QueryParam("wf_users") String wf_users){
		
		if(tm_users!=null && tm_when!=null){
			this.authorsThatMentionedSubquery(tm_al, tm_when, tm_users);
		}
		
		if(wf_users!=null){
			this.authorsWhoFollowSubquery(wf_al, wf_users);
		}
		
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users that mentioned " +
		"at least: " + tm_al +
		" when: " + tm_when +
		" amongst: " + tm_users +
		" and who follow " +
		"at least: " + wf_al +
		" amongst: " + wf_users +
		" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/thatMentioned")
	public Response usersThatMentioned(
		@QueryParam("atLeast") int tm_al,
		@QueryParam("when") String tm_when,
		@QueryParam("users") String tm_users){
		
		this.authorsThatMentionedSubquery(tm_al, tm_when, tm_users);
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users that mentioned: " +
		"at least: " + tm_al +
		" when: " + tm_when +
		" amongst: " + tm_users +
		" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	@GET
	@Path("/users/whoFollow")
	public Response usersWhoFollow(
		@QueryParam("atLeast") int wf_al,
		@QueryParam("users") String wf_users){
			
		this.authorsWhoFollowSubquery(wf_al, wf_users);
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users who follow: " +
		"at least: " + wf_al +
		" amongst: " + wf_users +
		" ----> " + result;
		return Response.status(200).entity(output).build();
	}
	
	private void authorsThatMentionedSubquery(int atLeast, String when, String users) {
		String[] splitted = users.split(",");
		Mention[] mentions = new Mention[splitted.length];
		for(int i=0; i<splitted.length; i++) {
			mentions[i] = new Mention(Long.parseLong(splitted[i]));
		}
		switch(when) {
			case "last_month":
				this.authors.thatMentioned(new LastMonth(), new AtLeast(atLeast), mentions);
			case "very_last_month":
				this.authors.thatMentioned(new LastMonthFromNow(), new AtLeast(atLeast), mentions);
		}
	}
	
	private void authorsWhoFollowSubquery(int atLeast, String users) {
		String[] splitted = users.split(",");
		Author[] auths = new Author[splitted.length];
		for(int i=0; i<splitted.length; i++) {
			auths[i] = new Author(Long.parseLong(splitted[i]));
		}
		this.authors.whoFollow(new AtLeast(atLeast), auths);
	}
 
}