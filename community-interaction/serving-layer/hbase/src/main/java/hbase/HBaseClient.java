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

	public void put(HTable table, String row, String colfam, String col,
					long ts, String value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts,
	            Bytes.toBytes(value));
		table.put(tableRow);		
	}
	
	/***/
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
	
	public Result get(HTable table, String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return table.get(tableRow);
	}
}
