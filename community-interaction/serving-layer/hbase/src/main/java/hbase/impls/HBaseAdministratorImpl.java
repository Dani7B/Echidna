package hbase.impls;

import hbase.HBaseAdministrator;

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

/**
 * Administrator to communicate with HBase for performing operations on HTables.
 * @author Daniele Morgantini
 * */
public class HBaseAdministratorImpl implements HBaseAdministrator {

	private Configuration config;
	
	private HBaseAdmin admin;
	
	private HConnection connection;
			

	/**
     * No argument contructor
     * @return an instance of the HBaseAdministrator with default configuration
     * @throws MasterNotRunningException 
	 * @throws ZooKeeperConnectionException */
	public HBaseAdministratorImpl() throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = HBaseConfiguration.create();
		this.admin = new HBaseAdmin(this.config);
		this.connection = HConnectionManager.createConnection(this.config);
	}
	
	
	/**
	 * Instantiates the client with the specified configuration
	 * @param config a specific configuration for HBase
     * @return an instance of the HBaseAdministrator with the specified configuration 
	 * @throws MasterNotRunningException 
	 * @throws ZooKeeperConnectionException */
	public HBaseAdministratorImpl(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = config;
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(config);
	}
	
	
	public HConnection getConnection() {
		return this.connection;
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
	
	
	public HTable getTable(String table) throws IOException {
	
		if (existsTable(table)) {
			return (HTable) this.connection.getTable(table);
		}
		return null;
	}
	
	
	public void deleteTable(String table) throws IOException {
	    
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
	    }
	}

}