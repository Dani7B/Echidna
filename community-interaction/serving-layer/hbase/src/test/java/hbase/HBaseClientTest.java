package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * Test class for HBaseClient
 */
public class HBaseClientTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClientTest.class);
    
    private HBaseClient client;
    
    private HTable testTable;
    
    @BeforeClass
    public void setUp() throws IOException {
		this.client = new HBaseClient();

    	if(this.client.existsTable("test"))
    		this.client.deleteTable("test");
    	this.client.createTable("test", "prova");
    	this.testTable = this.client.getTable("test");
    }
    
    @Test
    public void hasCreatedTable() throws IOException {
    	
    	this.client.createTable("testing", "prova");
    	boolean value = this.client.existsTable("testing");
    	assertTrue(value);
    	
    	HTable table = this.client.getTable("testing");
    	assertNotNull(table);
    }
    
    @Test
    public void hasDeletedTable() throws IOException {
            
            this.client.deleteTable("testing");
            boolean value = this.client.existsTable("testing");
            assertFalse(value);
            
            HTable table = this.client.getTable("testing");
            assertNull(table);
    }
    
    @Test
    public void hasInsertedRow() throws IOException {
    	
    	LOGGER.info("Auto flush: " + this.testTable.isAutoFlush());
    	
    	this.client.put(this.testTable, "row1", "prova", "col1", System.currentTimeMillis(), "50000");
    	
    	Result result = this.client.get(this.testTable, "row1");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    	
    	this.client.put(this.testTable, "row1", "prova", "col2", System.currentTimeMillis(), "51");
    	result = this.client.get(this.testTable, "row1");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2); //it's the size of the cell
    	
    	this.client.put(this.testTable, "row2", "prova", "col1", System.currentTimeMillis(), "16");
    	result = this.client.get(this.testTable, "row2");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    }
    
    @Test
    public void hasInsertedRows() throws IOException {
    	
    }
    
}
