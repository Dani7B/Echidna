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
	
	public static synchronized HBaseClientFactory getInstance() {
		if(instance == null)
			instance = new HBaseClientFactory();
		return instance;
	}

	
	public HBaseClient getHBaseClient(String tableName) {
		
		if(tableName.equalsIgnoreCase("mentionedBy"))
			return this.mentionedBy;
		if(tableName.equalsIgnoreCase("followedBy"))
			return this.followedBy;
		return null;
	}

}
