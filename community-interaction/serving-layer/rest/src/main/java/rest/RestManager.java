package rest;

import java.util.ArrayList;
import java.util.List;

import hbase.query.AtLeast;
import hbase.query.Author;
import hbase.query.Authors;
import hbase.query.HQuery;
import hbase.query.Mention;
import hbase.query.time.LastMonth;

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
		@QueryParam("tm_users") String tm_users){
			
		List<Mention> users = new ArrayList<Mention>();
		for(String s : tm_users.split(","))
			users.add(new Mention(Long.parseLong(s)));
		Mention[] mentions = new Mention[3];
		if(tm_when.equalsIgnoreCase("last_month"))
			this.authors.thatMentioned(new LastMonth(), new AtLeast(tm_al), users.toArray(mentions));
		this.authors = this.query.answer();
		
		List<Long> result = new ArrayList<Long>();
		for(Author a : this.authors.getAuthors())
			result.add(a.getId());
		String output = "Users that mentioned: " +
		"at least: " + tm_al +
		" when: " + tm_when +
		" amongst: " + result;
		return Response.status(200).entity(output).build();
	}
 
}