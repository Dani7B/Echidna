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
 * Implementation of HBaseAdministrator to get HBase to perform operations on HTables.
 * @author Daniele Morgantini
 * */
public class HTableAdmin implements HBaseAdministrator {
	
	private HBaseAdmin admin;
	
	private HConnection connection;
			

	/**
     * No argument constructor
     * @return an instance of the HBaseAdministrator with the default configuration
     * @throws MasterNotRunningException 
	 * @throws ZooKeeperConnectionException */
	public HTableAdmin() throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this(HBaseConfiguration.create());
	}
	
	
	/**
	 * Instantiates the client with the specified configuration
	 * @param config a specific configuration for HBase
     * @return an instance of the HBaseAdministrator with the specified configuration 
	 * @throws MasterNotRunningException 
	 * @throws ZooKeeperConnectionException */
	public HTableAdmin(final Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(config);
	}
	
	/** Returns the connection to HBase 
	 * @return the connection to HBase */
	public HConnection getConnection() {
		return this.connection;
	}
	
	
	@Override
	public void createTable(final String table, final String... colfams) throws IOException {
		
		if(!existsTable(table)) {
		
			HTableDescriptor desc = new HTableDescriptor(table);
			
		    for (String cf : colfams) {
		      HColumnDescriptor coldef = new HColumnDescriptor(cf);
		      desc.addFamily(coldef);
		    }
		    
		    this.admin.createTable(desc);
	    }
	}
	
	
	@Override
	public boolean existsTable(final String table) throws IOException {
		
		return this.admin.tableExists(table);
	}
	
	
	@Override
	public void disableTable(final String table) throws IOException {
	    this.admin.disableTable(table);
	}
	
	
	@Override
	public HTable getTable(final String table) throws IOException {
	
		if (existsTable(table)) {
			return (HTable) this.connection.getTable(table);
		}
		return null;
	}
	
	
	@Override
	public void deleteTable(final String table) throws IOException {
	    
		if (existsTable(table)) {
			disableTable(table);
			this.admin.deleteTable(table);
	    }
	}

}