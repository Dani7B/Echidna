package hbase.impls;

import hbase.HBaseClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of HBaseClient to communicate with HBase for perfoming CRUD operations on HTable rows.
 * @author Daniele Morgantini
 * */
public class HTableManager implements HBaseClient {
				   
	private HTable table;
	
    /**
     * No argument contructor
     * @return an instance of the HBaseClient */
	public HTableManager(HTable table){
		this.table = table;
	}
	
	
	@Override
	public void put(String row, String colfam, String col, long ts, byte[] value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts, value);
		this.table.put(tableRow);		
	}
	
	
	@Override
	public void put(String[] rows, String[] colfams, String[] cols, long[] tss, byte[][] values) 
			throws IOException {
		
		List<Put> tableRows = new ArrayList<Put>();
		for(int i=0; i<rows.length; i++) {
			Put tableRow = new Put(Bytes.toBytes(rows[i]));
			tableRow.add(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i], values[i]);
			tableRows.add(tableRow);
		}
		this.table.put(tableRows);
	}
	
	
	@Override
	public boolean exists(String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return this.table.exists(tableRow);
	}

	
	@Override
	public Result get(String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(String row, String[] columnFamilies, long[] timeRange, int maxVersions) 
			throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions(maxVersions);
		
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(String row, String[] columnFamilies, long timeStamp, int maxVersions) 
			throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions(maxVersions);
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		return this.table.get(tableRow);
	}	
	
	
	@Override
	public Result get(String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);		
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(String row, String[] columnFamilies, long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions();
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(String row, String[] columnFamilies, long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions();
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(String row, String[] columnFamilies) throws IOException {
		
		Get tableRow = prepareGetHistory(row, columnFamilies);
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result[] get(String[] rows, String[] columnFamilies, long[] timeRange) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, 0, timeRange);
			gets.add(tableRow);
		}
		return this.table.get(gets);
	}
	
	
	@Override
	public Result[] get(String[] rows, String[] columnFamilies, long timeStamp) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, timeStamp, null);
			gets.add(tableRow);
		}
		return this.table.get(gets);
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
	
	
	@Override
	public void delete(String row, String colfam, String col,	long ts) throws IOException {
		
		Delete tableRow = new Delete(Bytes.toBytes(row));
		tableRow.deleteColumns(Bytes.toBytes(colfam), Bytes.toBytes(col), ts);
		this.table.delete(tableRow);		
	}
	
	
	@Override
	public void delete(String row) throws IOException {

		Delete tableRow = new Delete(Bytes.toBytes(row));
		this.table.delete(tableRow);		
	}
	
	
	@Override
	public void delete(String[] rows, String[] colfams, String[] cols, long[] tss) throws IOException {
		
		List<Delete> tableRows = new ArrayList<Delete>();
		
		for(int i=0; i<rows.length; i++) {
			Delete tableRow = new Delete(Bytes.toBytes(rows[i]));
			tableRow.deleteColumns(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i]);
			tableRows.add(tableRow);
		}
		this.table.delete(tableRows);
	}


	@Override
	public Result[] scan(byte[] lowerRow, byte[] upperRow, byte[] lowerValue, byte[] upperValue) 
			throws IOException {
				
		Scan scan = new Scan(lowerRow,upperRow);
		
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		Filter qualifierFilter1 = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(lowerValue));
		fList.addFilter(qualifierFilter1);
		
		Filter qualifierFilter2 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(upperValue));
		fList.addFilter(qualifierFilter2);
		
		scan.setFilter(fList);
		
		ResultScanner scanner = this.table.getScanner(scan);
		List<Result> results = new ArrayList<Result>();
		for (Result res : scanner) {
			results.add(res);
		}
		scanner.close();
		
		Result[] finalResult = new Result[results.size()];
		return results.toArray(finalResult);
	}
	
	
	@Override
	public Result get(byte[] row, byte[] lowerValue, byte[] upperValue) throws IOException {
				
		Get tableRow = new Get(row);
				
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(lowerValue));
		fList.addFilter(filter1);
		
		Filter filter2 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(upperValue));
		fList.addFilter(filter2);
		
		tableRow.setFilter(fList);
		
		return this.table.get(tableRow);
	}
	
	@Override
	public Result get(byte[] row, byte[][] qualifiers) throws IOException {
				
		Get tableRow = new Get(row);
		for(HColumnDescriptor cf: this.table.getTableDescriptor().getColumnFamilies()) {
			for(byte[] q : qualifiers) {
				tableRow.addColumn(cf.getName(), q);
			}
		}
				
		return this.table.get(tableRow);
	}
}
