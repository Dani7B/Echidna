package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

/**
 * Client to communicate with HBase for perfoming CRUD operations on HTable rows.
 * @author Daniele Morgantini
 * */
public interface HBaseClient {
	
	/**
	 * Single put to insert a row into the specified table.
	 * @param row the row key
	 * @param colfam the name of the column family
	 * @param col the column name
	 * @param ts the timestamp
	 * @param value the value in byte */
	public abstract void put(final String row, final String colfam, final String col,
								final long ts, final byte[] value) throws IOException;

	
	/**
	 * Batch put to insert rows into the specified table.
	 * All the arrays have the same length. At the i-th index the values belong to the same row.
	 * @param rows the row keys
	 * @param colfams the names of the column families
	 * @param cols the column names
	 * @param tss the timestamps
	 * @param values the values */
	public abstract void put(final String[] rows, final String[] colfams, final String[] cols,
								final long[] tss, final byte[][] values) throws IOException;

	
	/**
	 * Checks the existence of a row in the HTable
	 * @return a flag to indicate if the row exists in the table
	 * @param row the row key */
	public abstract boolean exists(final String row) throws IOException;

	
	/**
	 * Single get to retrieve the latest version of all the columns for a row
	 * @return the result of the get query
	 * @param row the row key */
	public abstract Result get(final String row) throws IOException;
	
	
	/**
	 * Single get to retrieve the latest version of all the columns for a row
	 * @return the result of the get query
	 * @param row the row key */
	public abstract Result get(final byte[] row) throws IOException;

	
	/**
	 * Single get to retrieve the desired versions of the specified columns for a row, iff in a time range
	 * @return the result of the get query, filtered with the specified fields and time range
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1])
	 * @param maxVersions the number of versions to retrieve */
	public abstract Result get(final String row, final String[] columnFamilies,
								final long[] timeRange, final int maxVersions) throws IOException;

	
	/**
	 * Single get to retrieve the desired versions of the specified columns for a row, filtered with a timestamp
	 * @return the result of the get query, filtered with the specified fields and timestamp
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve
	 * @param maxVersions the number of versions to retrieve */
	public abstract Result get(final String row, final String[] columnFamilies, 
								final long timeStamp, final int maxVersions) throws IOException;

	
	/**
	 * Single get to retrieve the latest version of the specified columns for the desired row, iff in a time range
	 * @return a single result of the get query, filtered with the specified fields and time range
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result get(final String row, final String[] columnFamilies, final long[] timeRange) throws IOException;

	
	/**
	 * Single get to retrieve the latest version of the specified columns for the desired row, filtered with a timestamp
	 * @return a single result of the get query, filtered with the specified fields and timestamp
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result get(final String row, final String[] columnFamilies, final long timeStamp) throws IOException;

	
	/**
	 * Single get to retrieve specified columns of the desired row, iff in a time range
	 * @return all the versions of the query result, filtered with the specified fields and time range
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result getHistory(final String row, final String[] columnFamilies, final long[] timeRange) throws IOException;

	
	/**
	 * Single get to retrieve specified columns of the desired row, filtered with a timestamp
	 * @return all the versions of the query result, filtered with the specified fields and timestamp
	 * @param row the row key
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result getHistory(final String row, final String[] columnFamilies, final long timeStamp) throws IOException;

	
	/**
	 * Single get to retrieve all the versions available for specified columns of the desired row
	 * @return all the versions of the specified columns of the query result
	 * @param row the row key
	 * @param columnFamilies the column families names */
	public abstract Result getHistory(final String row, final String[] columnFamilies) throws IOException;

	
	/**
	 * Batch get to retrieve specified columns in a precise time range for the desired rows
	 * @return multiple results for the get queries, filtered with the specified fields and time range
	 * @param rows the row keys
	 * @param columnFamilies the column families names
	 * @param timeRange the specified timestamp range: [timeRange[0], timeRange[1]) */
	public abstract Result[] get(final String[] rows, final String[] columnFamilies, final long[] timeRange) throws IOException;

	
	/**
	 * Batch get to retrieve specified columns at a precise timestamp for the desired rows
	 * @return multiple results for the get queries, filtered with the specified fields and timestamp
	 * @param rows the row keys
	 * @param columnFamilies the column families names
	 * @param timeStamp the timeStamp identifying the version to retrieve */
	public abstract Result[] get(final String[] rows, final String[] columnFamilies, final long timeStamp) throws IOException;
	
	
	/**
	 * Single delete to erase a row from the specified table.
	 * @param row the row key
	 * @param colfam the name of the column family
	 * @param col the column name
	 * @param ts the timestamp */
	public abstract void delete(final String row, final String colfam, final String col, final long ts) throws IOException;

	
	/**
	 * Delete all columns, all versions of the row
	 * @param row the row key */
	public abstract void delete(final String row) throws IOException;

	
	/**
	 * Batch delete to erase rows from the specified table.
	 * All the arrays have the same length. At the i-th index the values belong to the same row.
	 * @param rows the row keys
	 * @param colfams the names of the column families
	 * @param cols the column names
	 * @param tss the timestamps */
	public abstract void delete(final String[] rows, final String[] colfams, 
									final String[] cols, final long[] tss) throws IOException;

	
	/**
	 * Scan to return the results in the row range and column range
	 * @return multiple results satisfying the query
	 * @param lowerRow the smallest row key to look for (included)
	 * @param upperRow the biggest row key to look for (excluded)
	 * @param lowerValue the smallest qualifier (column) value in the range (included)
	 * @param upperValue the biggest qualifier (column) value in the range (excluded) */
	public abstract Result[] scan(final byte[] lowerRow, final byte[] upperRow,
									final byte[] lowerValue, final byte[] upperValue) throws IOException;
	
	
	/**
	 * Scan to return the results in the row range and column range
	 * @return multiple results satisfying the query
	 * @param lowerRow the smallest row key to look for (included)
	 * @param upperRow the biggest row key to look for (excluded)
	 * @param lowerValue the smallest qualifier (column) value in the range (included)
	 * @param upperValue the biggest qualifier (column) value in the range (excluded) 
	 * @param allowedValues the list of allowed values to consider */
	public abstract Result[] scan(final byte[] lowerRow, final byte[] upperRow, final byte[] lowerValue,
									final byte[] upperValue, final byte[][] allowedValues) throws IOException;
	
	
	/**
	 * Scan to return the results in the row range for specified columns
	 * @return multiple results satisfying the query
	 * @param lowerRow the smallest row key to look for (included)
	 * @param upperRow the biggest row key to look for (excluded)
	 * @param qualifiers the array of the specified columns to look for */
	public abstract Result[] scan(final byte[] lowerRow, final byte[] upperRow, 
									final byte[][] qualifiers) throws IOException;
	
	
	/**
	 * Single get to return the results in the column range
	 * @return the results satisfying the query
	 * @param row the row key
	 * @param lowerValue the smallest qualifier (column) value in the range (included)
	 * @param upperValue the biggest qualifier (column) value in the range (excluded) */
	public abstract Result get(final byte[] row, final byte[] lowerValue, final byte[] upperValue) throws IOException;
	
	
	/**
	 * Single get to return the results belonging to specified columns
	 * @return the results satisfying the query
	 * @param row the row key
	 * @param qualifiers the array of the specified columns to look for */
	public abstract Result get(final byte[] row, final byte[][] qualifiers) throws IOException;
	
	
	/**
	 * Single get to return the results belonging to specified columns
	 * @return the results satisfying the query
	 * @param row the row key
	 * @param qualifiers the array of the specified columns to look for
	 * @param min the minimum allowed value */
	public abstract Result get(final byte[] row, final byte[][] qualifiers, final byte[] min) throws IOException;

}
