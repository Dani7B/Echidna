package rest;

import hbase.query.HQuery;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
 
@Path("/hello")
public class RestManager {
	
	private HQuery query;
 
	public RestManager() {
		query = new HQuery();
	}
	
	@GET
	@Path("/users")
	public Response getMsg(
		@QueryParam("msg") String msg ){
			
			String output = "Jersey says : " + msg;
			return Response.status(200).entity(output).build();
		
	}
 
}