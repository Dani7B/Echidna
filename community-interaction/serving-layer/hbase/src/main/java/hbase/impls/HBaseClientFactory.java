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

	private HBaseClient follow;
	
	private HBaseClient whoseFollowersFollow;
	
	private HBaseClient whoseFollowersAreFollowedBy;
	
	private HBaseClient whoseFollowersMentionedByMonth;
	
	private HBaseClient whoseFollowersMentionedByDay;
	
	private static int LARGEBATCH = 50;
	private static int MEDIUMBATCH = 25;
	private static int SMALLBATCH = 15;

	
	private HBaseClientFactory() {
		try {
			this.admin = new HTableAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private HBaseClient createHBaseClient(final String tableName, final int batching) {
		HBaseClient client = null;
		try {
			client = new HTableManager(this.admin.getTable(tableName), batching);
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
		this.mentionedBy = this.instantiateClient(this.mentionedBy, "mentionedBy", SMALLBATCH);
		return this.mentionedBy ;
	}
	
	public HBaseClient getMentionedByMonth() {
		this.mentionedByMonth = this.instantiateClient(this.mentionedByMonth, "mentionedByMonth", MEDIUMBATCH);
		return this.mentionedByMonth;
	}
	
	public HBaseClient getMentionedByDay() {
		this.mentionedByDay = this.instantiateClient(this.mentionedByDay, "mentionedByDay", LARGEBATCH);
		return this.mentionedByDay;
	}
	
	public HBaseClient getFollowedBy() {
		this.followedBy = this.instantiateClient(this.followedBy, "followedBy", MEDIUMBATCH);
		return this.followedBy;
	}

	public HBaseClient getFollow() {
		this.follow = this.instantiateClient(this.follow, "follow", MEDIUMBATCH);
		return this.follow;
	}
	
	public HBaseClient getWhoseFollowersFollow() {
		this.whoseFollowersFollow = this.instantiateClient(this.whoseFollowersFollow, "wff", LARGEBATCH);
		return this.whoseFollowersFollow;
	}
	
	public HBaseClient getWhoseFollowersAreFollowedBy() {
		this.whoseFollowersAreFollowedBy = this.instantiateClient(this.whoseFollowersAreFollowedBy, "wfafb", LARGEBATCH);
		return this.whoseFollowersAreFollowedBy;
	}
	
	public HBaseClient getWhoseFollowersMentionedByMonth() {
		this.whoseFollowersMentionedByMonth = this.instantiateClient(this.whoseFollowersMentionedByMonth, 
												"wfmByMonth", LARGEBATCH);
		return this.whoseFollowersMentionedByMonth;
	}
	
	public HBaseClient getWhoseFollowersMentionedByDay() {
		this.whoseFollowersMentionedByDay = this.instantiateClient(this.whoseFollowersMentionedByDay,
												"wfmByDay", MEDIUMBATCH);
		return this.whoseFollowersMentionedByDay;
	}
	
	
	private HBaseClient instantiateClient(final HBaseClient table, final String name, final int batching) {
		HBaseClient client = table;
		if(client == null)
			client = createHBaseClient(name, batching);
		return client;
	}
}
