package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Administrator to get HBase to perform operations on HTables.
 * @author Daniele Morgantini
 * */
public interface HBaseAdministrator {

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