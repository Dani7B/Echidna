package hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Administrator to get HBase to perform operations on HTables.
 * @author Daniele Morgantini
 * */
public interface HBaseAdministrator {

	public int PRIORITY_HIGHEST = Coprocessor.PRIORITY_HIGHEST;
	public int PRIORITY_SYSTEM = Coprocessor.PRIORITY_SYSTEM;
	public int PRIORITY_USER = Coprocessor.PRIORITY_USER;
	public int PRIORITY_LOWEST = Coprocessor.PRIORITY_LOWEST;
	/**
	 * Returns the connection to the HBase cluster 
	 * @return the cluster connection */
	public abstract HConnection getConnection();

	/**
	 * Creates a table with the specified name and column families
	 * @param table the table name
	 * @param colfams the names of the column families 
	 * @throws IOException */
	public abstract void createTable(final String table, final String... colfams)
			throws IOException;

	/**
	 * Creates a table with the specified name and column families
	 * @param table the table name
	 * @param coprocessorName the coprocessor full class name
	 * @param jarPath the path to the jar containing the coprocessor
	 * @param priority the coprocessor priority
	 * @param params arbitrary key-value parameter pairs passed into the coprocessor
	 * @param colfams the names of the column families 
	 * @throws IOException */
	public abstract void createTable(final String table, final String coprocessorName, final String jarPath,
			final int priority, final Map<String,String> params, final String... colfams)
			throws IOException;
	
	/**
	 * Checks existence of the table with the specified name
	 * @param table the table name 
	 * @throws IOException */
	public abstract boolean existsTable(final String table) throws IOException;

	/**
	 * Disables the table with the specified name
	 * @param table the table name
	 * @throws IOException */
	public abstract void disableTable(final String table) throws IOException;

	/**
	 * Retrieves the HTable to query the specified table
	 * @param table the table name 
	 * @return the HTable with the specified name
	 * @throws IOException */
	public abstract HTable getTable(final String table) throws IOException;

	/**
	 * Deletes the table with the specified name
	 * @param table the table name
	 * @throws IOException */
	public abstract void deleteTable(final String table) throws IOException;

}