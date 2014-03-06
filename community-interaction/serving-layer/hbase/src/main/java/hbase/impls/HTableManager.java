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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of HBaseClient to communicate with HBase for perfoming CRUD operations on HTable rows.
 * @author Daniele Morgantini
 * */
public class HTableManager implements HBaseClient {
				   
	private HTable table;
	
	private int batching;
	
    /**
     * No argument contructor
     * @return an instance of the HBaseClient */
	public HTableManager(final HTable table, final int batching){
		this.table = table;
		this.batching = batching;
	}
	
	
	@Override
	public void put(final String row, final String colfam, final String col, 
						final long ts, final byte[] value) throws IOException {
		
		Put tableRow = new Put(Bytes.toBytes(row));
		
		tableRow.add(Bytes.toBytes(colfam), Bytes.toBytes(col), ts, value);
		this.table.put(tableRow);		
	}
	
	
	@Override
	public void put(final String[] rows, final String[] colfams, final String[] cols, 
						final long[] tss, final byte[][] values) 
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
	public boolean exists(final String row) throws IOException {
		
		Get tableRow = new Get(Bytes.toBytes(row));
		return this.table.exists(tableRow);
	}

	
	@Override
	public Result get(final String row) throws IOException {
		
		return this.get(Bytes.toBytes(row));
	}
	
	
	@Override
	public Result get(final byte[] row) throws IOException {
		
		byte[][] allowedValues = new byte[0][];
		return this.get(row, allowedValues);
	}
	
	
	@Override
	public Result get(final byte[] row, final byte[] min) throws IOException {
		
		Get tableRow = new Get(row);
		
		Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(min));
		tableRow.setFilter(valueFilter);
				
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(final String row, final String[] columnFamilies, 
							final long[] timeRange, final int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions(maxVersions);
		
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(final String row, final String[] columnFamilies, 
						final long timeStamp, final int maxVersions) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions(maxVersions);
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result get(final String row, final String[] columnFamilies, final long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		return this.table.get(tableRow);
	}	
	
	
	@Override
	public Result get(final String row, final String[] columnFamilies, final long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);		
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(final String row, final String[] columnFamilies, final long[] timeRange) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, 0, timeRange);
		tableRow.setMaxVersions();
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(final String row, final String[] columnFamilies, final long timeStamp) throws IOException {
		
		Get tableRow = prepareGet(row, columnFamilies, timeStamp, null);
		tableRow.setMaxVersions();
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result getHistory(final String row, final String[] columnFamilies) throws IOException {
		
		Get tableRow = prepareGetHistory(row, columnFamilies);
		return this.table.get(tableRow);
	}
	
	
	@Override
	public Result[] get(final String[] rows, final String[] columnFamilies, final long[] timeRange) throws IOException {
		
		List<Get> gets = new ArrayList<Get>();
		Get tableRow = null;
		for(String row : rows) {
			tableRow = prepareGet(row, columnFamilies, 0, timeRange);
			gets.add(tableRow);
		}
		return this.table.get(gets);
	}
	
	
	@Override
	public Result[] get(final String[] rows, final String[] columnFamilies, final long timeStamp) throws IOException {
		
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
	private Get prepareGet(final String row, final String[] columnFamilies,	
								final long timeStamp, final long[] timeRange) throws IOException {
		
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
	private Get prepareGetHistory(final String row, final String[] columnFamilies) throws IOException {
		
		Get get = new Get(Bytes.toBytes(row));
		
		if(columnFamilies != null)
			for(String cf : columnFamilies)
				get.addFamily(Bytes.toBytes(cf));
		
		get.setMaxVersions();
		return get;
	}
	
	
	@Override
	public void delete(final String row, final String colfam, final String col, final long ts) throws IOException {
		
		Delete tableRow = new Delete(Bytes.toBytes(row));
		tableRow.deleteColumns(Bytes.toBytes(colfam), Bytes.toBytes(col), ts);
		this.table.delete(tableRow);		
	}
	
	
	@Override
	public void delete(final String row) throws IOException {

		Delete tableRow = new Delete(Bytes.toBytes(row));
		this.table.delete(tableRow);		
	}
	
	
	@Override
	public void delete(final String[] rows, final String[] colfams, final String[] cols, final long[] tss)
				throws IOException {
		
		List<Delete> tableRows = new ArrayList<Delete>();
		
		for(int i=0; i<rows.length; i++) {
			Delete tableRow = new Delete(Bytes.toBytes(rows[i]));
			tableRow.deleteColumns(Bytes.toBytes(colfams[i]), Bytes.toBytes(cols[i]), tss[i]);
			tableRows.add(tableRow);
		}
		this.table.delete(tableRows);
	}


	@Override
	public Result[] scan(final byte[] lowerRow, final byte[] upperRow, final byte[] lowerValue,
							final byte[] upperValue) throws IOException {
		
		byte[][] allowedValues = new byte[0][];
		return this.scan(lowerRow, upperRow, lowerValue, upperValue, allowedValues);
	}
	
	
	@Override
	public Result[] scan(final byte[] lowerRow, final byte[] upperRow, final byte[] lowerValue,
							final byte[] upperValue, final byte[][] allowedValues) throws IOException {
				
		Scan scan = new Scan(lowerRow,upperRow);
				
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		Filter qualifierFilter1 = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(lowerValue));
		fList.addFilter(qualifierFilter1);
		
		Filter qualifierFilter2 = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(upperValue));
		fList.addFilter(qualifierFilter2);
		
		if(allowedValues.length!=0){
			FilterList valueList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	
			for(byte[] value : allowedValues) {
				Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
						new BinaryComparator(value));
				valueList.addFilter(valueFilter);
			}
			fList.addFilter(valueList);
		}
		scan.setFilter(fList);
		scan.setBatch(batching);
				
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
	public Result get(final byte[] row, final byte[] lowerValue, final byte[] upperValue) throws IOException {
				
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
	public Result get(final byte[] row, final byte[][] qualifiers) throws IOException {
				
		Get tableRow = new Get(row);
		for(HColumnDescriptor cf: this.table.getTableDescriptor().getColumnFamilies()) {
			for(byte[] q : qualifiers) {
				tableRow.addColumn(cf.getName(), q);
			}
		}
				
		return this.table.get(tableRow);
	}
	
	@Override
	public Result get(final byte[] row, final byte[][] qualifiers, final byte[] min) throws IOException {
		
		Get tableRow = new Get(row);
		for(HColumnDescriptor cf: this.table.getTableDescriptor().getColumnFamilies()) {
			for(byte[] q : qualifiers) {
				tableRow.addColumn(cf.getName(), q);
			}
		}
		
		Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(min));
		tableRow.setFilter(valueFilter);
		
		return this.table.get(tableRow);
	}


	@Override
	public Result[] scan(byte[] lowerRow, byte[] upperRow, byte[][] qualifiers)
			throws IOException {
		
		if(lowerRow == upperRow) {
			Result[] results = new Result[1];
			results[0] = this.get(upperRow, qualifiers);
			return results;
		}
		
		Scan scan = new Scan(lowerRow,upperRow);
		
		for(HColumnDescriptor cf: this.table.getTableDescriptor().getColumnFamilies()) {
			for(byte[] q : qualifiers) {
				scan.addColumn(cf.getName(), q);
			}
		}
		
		scan.setBatch(batching);
				
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
	public Result[] scan(byte[] lowerRow, byte[] upperRow, byte[][] qualifiers,
			byte[] min) throws IOException {
		
		if(lowerRow == upperRow) {
			Result[] results = new Result[1];
			results[0] = this.get(upperRow, qualifiers,min);
			return results;
		}
		
		Scan scan = new Scan(lowerRow,upperRow);
		
		for(HColumnDescriptor cf: this.table.getTableDescriptor().getColumnFamilies()) {
			for(byte[] q : qualifiers) {
				scan.addColumn(cf.getName(), q);
			}
		}
		
		Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(min));
		scan.setFilter(valueFilter);
		scan.setBatch(batching);
				
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
	public Result[] scanPrefix(byte[] lowerRow, byte[] upperRow,
			byte[][] qualifiersPrefix, byte[] min) throws IOException {
		
		Scan scan = new Scan(lowerRow,upperRow);
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		FilterList qualifierFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		for(byte[] prefix : qualifiersPrefix) {
			Filter prefixFilter = new ColumnPrefixFilter(prefix);
			qualifierFilters.addFilter(prefixFilter);
		}
		fList.addFilter(qualifierFilters);
		
		Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(min));
		fList.addFilter(valueFilter);
		
		scan.setFilter(fList);
		scan.setBatch(batching);
				
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
	public Result[] scanPrefix(byte[] lowerRow, byte[] upperRow, byte[][] qualifiersPrefix) throws IOException {
		
		Scan scan = new Scan(lowerRow,upperRow);
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		for(byte[] prefix : qualifiersPrefix) {
			Filter prefixFilter = new ColumnPrefixFilter(prefix);
			fList.addFilter(prefixFilter);
		}
		
		scan.setFilter(fList);
		scan.setBatch(batching);
				
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
	public Result getPrefix(byte[] row, byte[][] qualifiersPrefix, byte[] min) throws IOException {
		
		Get tableRow = new Get(row);
		
		FilterList qualifierFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		for(byte[] prefix : qualifiersPrefix) {
			Filter prefixFilter = new ColumnPrefixFilter(prefix);
			qualifierFilter.addFilter(prefixFilter);
		}
		
		FilterList fList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		fList.addFilter(qualifierFilter);
		Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(min));
		fList.addFilter(valueFilter);
		tableRow.setFilter(fList);
		
		return this.table.get(tableRow);
	}


	@Override
	public Result[] scan(byte[] lowerRow, byte[] upperRow) throws IOException {
		byte[][] qualifiers = new byte[0][];
		return this.scan(lowerRow, upperRow, qualifiers);
	}
}
