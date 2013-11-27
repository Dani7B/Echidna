package hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotEquals;

/**
 * Test class for HBaseClient
 */
public class HBaseClientTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClient.class);
    
    private HBaseClient client;
    
    @BeforeClass
    public void setUp() throws MasterNotRunningException, ZooKeeperConnectionException {
    	
    	this.client = new HBaseClient();
    }
    
    @Test
    public void hasCreatedTable() throws IOException {
    	
    	this.client.createTable("testing", "prova");
    	
    	boolean value = this.client.existsTable("testing");
    	
    	assertTrue(value);
    	
    	HTable table = this.client.getTable("testing");
    	
    	assertNotEquals(null, table);
    	
    }
}
