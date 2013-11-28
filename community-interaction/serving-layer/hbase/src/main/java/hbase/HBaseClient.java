package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * */
public class HBaseClient {
		
	private Configuration config;
	
	private HBaseAdmin admin;
	
	private HConnection connection;
	
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);
		
    
	public HBaseClient() throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = HBaseConfiguration.create();
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(this.config);
	}
	
	public HBaseClient(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = config;
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(config);
	}
	
	public void createTable(String table, String... colfams) throws IOException {
		
		if(!existsTable(table)) {
		
			HTableDescriptor desc = new HTableDescriptor(table);
			
		    for (String cf : colfams) {
		      HColumnDescriptor coldef = new HColumnDescriptor(cf);
		      desc.addFamily(coldef);
		    }
		    
		    admin.createTable(desc);
	    }
	}
	
	public boolean existsTable(String table) throws IOException {
		
		return admin.tableExists(table);
	}
	
	public void disableTable(String table) throws IOException {
	    admin.disableTable(table);
	}
	
	public void deleteTable(String table) throws IOException {
	    
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
	    }
	}
	
	public HTable getTable(String table) throws IOException {
	
		if (existsTable(table)) {
			return (HTable) this.connection.getTable(table);
		}
		return null;
	}

}
