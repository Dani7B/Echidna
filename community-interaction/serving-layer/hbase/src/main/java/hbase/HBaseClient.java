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
	 * @param value the value in byte */
	public void put(HTable table, String row, String colfam, String col,
					long ts, byte[] value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts, value);
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
            long[] tss, byte[][] values) throws IOException {
		
		List<Put> tableRows = new ArrayList<Put>();
		for(int i=0; i<rows.length; i++) {
			
			Put tableRow = new Put(Bytes.toBytes(rows[i]));
			tableRow.add(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i], values[i]);
			
			tableRows.add(tableRow);
		}
		
		table.put(tableRows);
	}
	
	
	/**
	 * @return the result of the get query
	 * @param table the HTable to query 
	 * @param row the id of the row */
	public Result get(HTable table, String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return table.get(tableRow);
	}
	
	
	/**
	 * @return the result of the get query, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1])
	 * @param maxVersions the number of versions to retrieve */
	public Result get(HTable table, String row, String[] columnFamilies,
				long[] timeRange, int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions(maxVersions);
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return the result of the get query, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve
	 * @param maxVersions the number of versions to retrieve */
	public Result get(HTable table, String row, String[] columnFamilies,
				long timeStamp, int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions(maxVersions);
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return a single result of the get query, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public Result get(HTable table, String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		return table.get(tableRow);
	}
	
	
	/**
	 * @return a single result of the get query, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public Result get(HTable table, String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return all the versions of the query result, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public Result getHistory(HTable table, String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions();
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return all the versions of the query result, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public Result getHistory(HTable table, String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions();
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return all the versions of the query result, filtered with the specified fields
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names */
	public Result getHistory(HTable table, String row, String[] columnFamilies) throws IOException {
		
		Get tableRow = prepareGetHistory(row, columnFamilies);
		
		return table.get(tableRow);
	}
	
	
	/**
	 * @return multiple results for the get queries, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param rows the ids of the rows
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public Result[] get(HTable table, String[] rows, String[] columnFamilies, long[] timeRange) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, 0, timeRange);
			gets.add(tableRow);
		}
		return table.get(gets);
	}
	
	
	/**
	 * @return multiple results for the get queries, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param rows the ids of the rows
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public Result[] get(HTable table, String[] rows, String[] columnFamilies, long timeStamp) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, timeStamp, null);
			gets.add(tableRow);
		}
		return table.get(gets);
	}
	
	
	/**
	 * @return the get query to be executed
	 * @param row the id of the row
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	private Get prepareGet(String row, String[] columnFamilies,	long timeStamp, long[] timeRange) throws IOException {
		
		Get get = new Get(Bytes.toBytes(row));
		
		if(columnFamilies != null)
			for(String cf : columnFamilies)
				get.addFamily(Bytes.toBytes(cf));
		
		if(timeRange == null)
			get.setTimeStamp(timeStamp);
		else
			get.setTimeRange(timeRange[0], timeRange[1]);
		
		return get;
	}
	
	
	/**
	 * @return the get query to be executed
	 * @param row the id of the row
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	private Get prepareGetHistory(String row, String[] columnFamilies) throws IOException {
		
		Get get = new Get(Bytes.toBytes(row));
		
		if(columnFamilies != null)
			for(String cf : columnFamilies)
				get.addFamily(Bytes.toBytes(cf));
		
		get.setMaxVersions();
		
		return get;
	}
}
