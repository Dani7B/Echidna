package rest;

import java.util.ArrayList;
import java.util.List;

import hbase.query.HQuery;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
 
@Path("/search")
public class RestManager {
	
	private HQuery query;
 
	public RestManager() {
		query = new HQuery();
	}
	
	@GET
	@Path("/users")
	public Response getMsg(
		@QueryParam("tm_atLeast") int tm_al,
		@QueryParam("tm_when") String tm_when,
		@QueryParam("tm_users") String tm_users){
			
		List<Long> users = new ArrayList<Long>();
		for(String s : tm_users.split(","))
			users.add(Long.parseLong(s));
		
			String output = "Users that mentioned: " +
			"at least: " + tm_al +
			" when: " + tm_when +
			" amongst: " + users;
			return Response.status(200).entity(output).build();
	}
 
}