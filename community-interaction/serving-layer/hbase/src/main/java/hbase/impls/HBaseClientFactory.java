package hbase.impls;

import java.io.IOException;

import hbase.HBaseAdministrator;
import hbase.HBaseClient;

public class HBaseClientFactory {
	
	private static HBaseClientFactory instance;

	private HBaseAdministrator admin;
	
	private HBaseClient mentionedBy;
	
	private HBaseClient followedBy;


	
	private HBaseClientFactory() {
		try {
			this.admin = new HTableAdmin();
			this.mentionedBy = new HTableManager(admin.getTable("mentionedBy"));
			this.followedBy = new HTableManager(admin.getTable("followedBy"));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private HBaseClient createHBaseClient(String tableName) {
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
		if(this.mentionedBy == null)
			this.mentionedBy = createHBaseClient("mentionedBy");
		return this.mentionedBy;
	}
	
	public HBaseClient getFollowedBy() {
		if(this.followedBy == null)
			this.followedBy = createHBaseClient("followedBy");
		return this.followedBy;
	}

}