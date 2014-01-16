package hbase.impls;

import java.io.IOException;

import hbase.HBaseAdministrator;
import hbase.HBaseClient;

/**
 * Simple factory to create single instances of HBaseClients.
 * It acts as support for the HSubQueries
 * @author Daniele Morgantini
 */
public class HBaseClientFactory {
	
	private static HBaseClientFactory instance;

	private HBaseAdministrator admin;
	
	private HBaseClient mentionedBy;
	
	private HBaseClient mentionedByMonth;
	
	private HBaseClient mentionedByDay;
	
	private HBaseClient followedBy;


	
	private HBaseClientFactory() {
		try {
			this.admin = new HTableAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private HBaseClient createHBaseClient(final String tableName) {
		HBaseClient client = null;
		try {
			client = new HTableManager(this.admin.getTable(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return client;
	}
	
	public static synchronized HBaseClientFactory getInstance() {
		if(instance == null)
			instance = new HBaseClientFactory();
		return instance;
	}

	public HBaseClient getMentionedBy() {
		this.mentionedBy = this.instantiateClient(this.mentionedBy, "mentionedBy");
		return this.mentionedBy ;
	}
	
	public HBaseClient getMentionedByMonth() {
		this.mentionedByMonth = this.instantiateClient(this.mentionedByMonth, "mentionedByMonth");
		return this.mentionedByMonth;
	}
	
	public HBaseClient getMentionedByDay() {
		this.mentionedByDay = this.instantiateClient(this.mentionedByDay, "mentionedByDay");
		return this.mentionedByDay;
	}
	
	public HBaseClient getFollowedBy() {
		this.followedBy = this.instantiateClient(this.followedBy, "followedBy");
		return this.followedBy;
	}

	private HBaseClient instantiateClient(final HBaseClient table, final String name) {
		HBaseClient client = table;
		if(client == null)
			client = createHBaseClient(name);
		return client;
	}
}
