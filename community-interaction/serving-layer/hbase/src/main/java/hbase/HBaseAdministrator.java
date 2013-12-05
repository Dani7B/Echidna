package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;

public interface HBaseAdministrator {

	public abstract HConnection getConnection();

	/**
	 * Creates a table with the specified name and column families
	 * @param table the table name
	 * @param colfams the names of the column families 
	 * @throws IOException */
	public abstract void createTable(String table, String... colfams)
			throws IOException;

	/**
	 * Checks existence of the table with the specified name
	 * @param table the table name 
	 * @throws IOException */
	public abstract boolean existsTable(String table) throws IOException;

	/**
	 * Disables the table with the specified name
	 * @param table the table name
	 * @throws IOException */
	public abstract void disableTable(String table) throws IOException;

	/**
	 * Retrieves the HTable to query the specified table
	 * @param table the table name 
	 * @return the HTable with the specified name
	 * @throws IOException */
	public abstract HTable getTable(String table) throws IOException;

	/**
	 * Deletes the table with the specified name
	 * @param table the table name
	 * @throws IOException */
	public abstract void deleteTable(String table) throws IOException;

}