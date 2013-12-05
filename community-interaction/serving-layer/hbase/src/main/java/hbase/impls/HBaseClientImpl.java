package hbase.impls;

import hbase.HBaseClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Client to communicate with HBase for perfoming CRUD operations.
 * @author Daniele Morgantini
 * */
public class HBaseClientImpl implements HBaseClient {
				   
    /**
     * No argument contructor
     * @return an instance of the HBaseClient */
	public HBaseClientImpl(){
		
	}
	
	
	public void put(HTable table, String row, String colfam, String col,
					long ts, byte[] value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts, value);
		table.put(tableRow);		
	}
	
	
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
	
	
	public boolean exists(HTable table, String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return table.exists(tableRow);
	}

	
	public Result get(HTable table, String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return table.get(tableRow);
	}
	
	
	public Result get(HTable table, String row, String[] columnFamilies,
				long[] timeRange, int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions(maxVersions);
		
		return table.get(tableRow);
	}
	
	
	public Result get(HTable table, String row, String[] columnFamilies,
				long timeStamp, int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions(maxVersions);
		return table.get(tableRow);
	}
	
	
	public Result get(HTable table, String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		return table.get(tableRow);
	}	
	
	
	public Result get(HTable table, String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);		
		return table.get(tableRow);
	}
	
	
	public Result getHistory(HTable table, String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions();
		return table.get(tableRow);
	}
	
	
	public Result getHistory(HTable table, String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions();
		return table.get(tableRow);
	}
	
	
	public Result getHistory(HTable table, String row, String[] columnFamilies) throws IOException {
		
		Get tableRow = prepareGetHistory(row, columnFamilies);
		return table.get(tableRow);
	}
	
	
	public Result[] get(HTable table, String[] rows, String[] columnFamilies, long[] timeRange) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, 0, timeRange);
			gets.add(tableRow);
		}
		return table.get(gets);
	}
	
	
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
	 * Prepares the get with all due fields, except for the max number of versions
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
	 * Prepares the get in case you want all the available versions of the row
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
	
	
	public void delete(HTable table, String row, String colfam, String col,	long ts) throws IOException {
		
		Delete tableRow = new Delete(Bytes.toBytes(row));
		tableRow.deleteColumns(Bytes.toBytes(colfam), Bytes.toBytes(col), ts);
		table.delete(tableRow);		
	}
	
	
	public void delete(HTable table, String row) throws IOException {

		Delete tableRow = new Delete(Bytes.toBytes(row));
		table.delete(tableRow);		
	}
	
	
	public void delete(HTable table, String[] rows, String[] colfams, String[] cols, long[] tss) throws IOException {
		
		List<Delete> tableRows = new ArrayList<Delete>();
		
		for(int i=0; i<rows.length; i++) {
			Delete tableRow = new Delete(Bytes.toBytes(rows[i]));
			tableRow.deleteColumns(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i]);
			tableRows.add(tableRow);
		}
		table.delete(tableRows);
	}
}
