package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public interface HBaseClient {

	/**
	 * Single put to insert a row into the specified table.
	 * @param table the HTable to insert the row in
	 * @param row the id of the row
	 * @param colfam the name of the column family
	 * @param col the column name
	 * @param ts the timestamp
	 * @param value the value in byte */
	public abstract void put(HTable table, String row, String colfam,
			String col, long ts, byte[] value) throws IOException;

	
	/**
	 * Batch put to insert rows into the specified table.
	 * All the arrays have the same length. At the i-th index the values belong to the same row.
	 * @param table the HTable to insert the rows in
	 * @param rows the ids of the rows
	 * @param colfams the names of the column families
	 * @param cols the column names
	 * @param tss the timestamps
	 * @param values the values */
	public abstract void put(HTable table, String[] rows, String[] colfams,
			String[] cols, long[] tss, byte[][] values) throws IOException;

	
	/**
	 * Checks the existence of a row in the HTable
	 * @return a flag to indicate if the row exists in the table
	 * @param table the HTable to query 
	 * @param row the id of the row */
	public abstract boolean exists(HTable table, String row) throws IOException;

	
	/**
	 * Single get to retrieve the latest version of all the columns for a row
	 * @return the result of the get query
	 * @param table the HTable to query 
	 * @param row the id of the row */
	public abstract Result get(HTable table, String row) throws IOException;

	
	/**
	 * Single get to retrieve the desired versions of the specified columns for a row, iff in a time range
	 * @return the result of the get query, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1])
	 * @param maxVersions the number of versions to retrieve */
	public abstract Result get(HTable table, String row,
			String[] columnFamilies, long[] timeRange, int maxVersions)
			throws IOException;

	
	/**
	 * Single get to retrieve the desired versions of the specified columns for a row, filtered with a timestamp
	 * @return the result of the get query, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve
	 * @param maxVersions the number of versions to retrieve */
	public abstract Result get(HTable table, String row,
			String[] columnFamilies, long timeStamp, int maxVersions)
			throws IOException;

	
	/**
	 * Single get to retrieve the latest version of the specified columns for the desired row, iff in a time range
	 * @return a single result of the get query, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result get(HTable table, String row,
			String[] columnFamilies, long[] timeRange) throws IOException;

	
	/**
	 * Single get to retrieve the latest version of the specified columns for the desired row, filtered with a timestamp
	 * @return a single result of the get query, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result get(HTable table, String row,
			String[] columnFamilies, long timeStamp) throws IOException;

	
	/**
	 * Single get to retrieve specified columns of the desired row, iff in a time range
	 * @return all the versions of the query result, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result getHistory(HTable table, String row,
			String[] columnFamilies, long[] timeRange) throws IOException;

	
	/**
	 * Single get to retrieve specified columns of the desired row, filtered with a timestamp
	 * @return all the versions of the query result, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result getHistory(HTable table, String row,
			String[] columnFamilies, long timeStamp) throws IOException;

	
	/**
	 * Single get to retrieve all the versions available for specified columns of the desired row
	 * @return all the versions of the specified columns of the query result
	 * @param table the HTable to query 
	 * @param row the id of the row 
	 * @param columnFamilies the column families names */
	public abstract Result getHistory(HTable table, String row,
			String[] columnFamilies) throws IOException;

	
	/**
	 * Batch get to retrieve specified columns in a precise time range for the desired rows
	 * @return multiple results for the get queries, filtered with the specified fields and time range
	 * @param table the HTable to query 
	 * @param rows the ids of the rows
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result[] get(HTable table, String[] rows,
			String[] columnFamilies, long[] timeRange) throws IOException;

	
	/**
	 * Batch get to retrieve specified columns at a precise timestamp for the desired rows
	 * @return multiple results for the get queries, filtered with the specified fields and timestamp
	 * @param table the HTable to query 
	 * @param rows the ids of the rows
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result[] get(HTable table, String[] rows,
			String[] columnFamilies, long timeStamp) throws IOException;
	
	/**
	 * Single delete to erase a row from the specified table.
	 * @param table the HTable to delete the row from
	 * @param row the id of the row
	 * @param colfam the name of the column family
	 * @param col the column name
	 * @param ts the timestamp */
	public abstract void delete(HTable table, String row, String colfam,
			String col, long ts) throws IOException;

	
	/**
	 * Delete all columns, all versions of the row
	 * @param table the HTable to delete the row from
	 * @param row the id of the row */
	public abstract void delete(HTable table, String row) throws IOException;

	
	/**
	 * Batch delete to erase rows from the specified table.
	 * All the arrays have the same length. At the i-th index the values belong to the same row.
	 * @param table the HTable to delete the rows from
	 * @param rows the ids of the rows
	 * @param colfams the names of the column families
	 * @param cols the column names
	 * @param tss the timestamps */
	public abstract void delete(HTable table, String[] rows, String[] colfams,
			String[] cols, long[] tss) throws IOException;

}
