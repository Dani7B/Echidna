package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * 
 * */
public class HBaseClient {
		
	private Configuration config;
	
	private HBaseAdmin admin;
		
	public HBaseClient() throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = HBaseConfiguration.create();
		this.admin = new HBaseAdmin(config);
	}
	
	public HBaseClient(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = config;
		this.admin = new HBaseAdmin(config);
	}
	
	public void createTable(String table, String... colfams) throws IOException {
		
		HTableDescriptor desc = new HTableDescriptor(table);
		
	    for (String cf : colfams) {
	      HColumnDescriptor coldef = new HColumnDescriptor(cf);
	      desc.addFamily(coldef);
	    }
	    
	    admin.createTable(desc);
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

}
