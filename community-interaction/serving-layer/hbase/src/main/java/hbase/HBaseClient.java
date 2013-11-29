package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client to communicate with HBase to perfom CRUD operations.
 * @author Daniele Morgantini
 * */
public class HBaseClient {
		
	private Configuration config;
	
	private HBaseAdmin admin;
	
	private HConnection connection;
	
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);
		
   
    /**
     * @return an instance of the HBaseClient with default configuration */
	public HBaseClient() throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = HBaseConfiguration.create();
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(this.config);
	}
	
	/**
	 * @param config a specific configuration for HBase
     * @return an instance of the HBaseClient with the specified configuration */
	public HBaseClient(Configuration config) throws MasterNotRunningException, ZooKeeperConnectionException {
		
		this.config = config;
		this.admin = new HBaseAdmin(config);
		this.connection = HConnectionManager.createConnection(config);
	}
	
	/**
	 * Creates a table with the specified name and column families
	 * @param table the table name
	 * @param colfams the names of the column families */
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
	
	
	/**
	 * Checks existence of the table with the specified name
	 * @param table the table name */
	public boolean existsTable(String table) throws IOException {
		
		return admin.tableExists(table);
	}
	
	
	/**
	 * Disables the table with the specified name
	 * @param table the table name */
	public void disableTable(String table) throws IOException {
	    admin.disableTable(table);
	}
	
	
	/**
	 * Deletes the table with the specified name
	 * @param table the table name */
	public void deleteTable(String table) throws IOException {
	    
		if (existsTable(table)) {
			disableTable(table);
			admin.deleteTable(table);
	    }
	}
	
	
	/**
	 * Retrieves the HTable to query the specified table
	 * @param table the table name 
	 * @return the HTable with the specified name */
	public HTable getTable(String table) throws IOException {
	
		if (existsTable(table)) {
			return (HTable) this.connection.getTable(table);
		}
		return null;
	}
	
	
	/**
	 * Single put to insert a row into the specified table.
	 * @param table the Htable to insert the row in
	 * @param row the id of the row
	 * @param colfam the name of the column family
	 * @param col the column name
	 * @param ts the timestamp
	 * @param value the value */
	public void put(HTable table, String row, String colfam, String col,
					long ts, String value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts,
	            Bytes.toBytes(value));
		table.put(tableRow);		
	}
	
	/**
	 * Batch put to insert rows into the specified table.
	 * All the arrays have the same length. At the i-th index the values belong to the same row.
	 * @param table the Htable to insert the rows in
	 * @param rows the ids of the rows
	 * @param colfams the names of the column families
	 * @param cols the column names
	 * @param tss the timestamps
	 * @param values the values */
	public void put(HTable table, String[] rows, String[] colfams, String[] cols,
            long[] tss, String[] values) throws IOException {
		
		List<Put> tableRows = new ArrayList<Put>();
		for(int i=0; i<rows.length; i++) {
			
			Put tableRow = new Put(Bytes.toBytes(rows[i]));
			tableRow.add(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i],
		            Bytes.toBytes(values[i]));
			
			tableRows.add(tableRow);
		}
		
		table.put(tableRows);
	}
	
	/**
	 * Return the result of the get query
	 * @param table the HTable to query 
	 * @param row the id of the row */
	public Result get(HTable table, String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return table.get(tableRow);
	}
}
