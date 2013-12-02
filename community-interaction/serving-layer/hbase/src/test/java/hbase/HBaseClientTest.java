package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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

    	if(this.client.existsTable("casa"))
    		this.client.deleteTable("casa");
    	
    	this.client.createTable("casa", "salone", "soggiorno");
    	this.testTable = this.client.getTable("casa");
    	this.testTable.setWriteBufferSize(20971520L); // Otherwise it doesn't get the default value from config
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
    	
    	this.client.put(this.testTable, "row1", "salone", "mq", System.currentTimeMillis(), Bytes.toBytes(25));
    	
    	Result result = this.client.get(this.testTable, "row1");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    	
    	this.client.put(this.testTable, "row1", "soggiorno", "luminoso", System.currentTimeMillis(), Bytes.toBytes(true));
    	result = this.client.get(this.testTable, "row1");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2); //it's the size of the cell
    	
    	this.client.put(this.testTable, "row2", "soggiorno", "mq", System.currentTimeMillis(), Bytes.toBytes(16));
    	result = this.client.get(this.testTable, "row2");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    }
    
    @Test
    public void hasInsertedRowWithoutAutoflush() throws IOException {
    	
    	this.testTable.setAutoFlush(false);
    	LOGGER.info("Auto flush: " + this.testTable.isAutoFlush());
    	
    	this.client.put(this.testTable, "row3", "soggiorno", "mq", System.currentTimeMillis(), Bytes.toBytes(15));
    	
    	Result result = this.client.get(this.testTable, "row3");    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),0);
    	
    	this.client.put(this.testTable, "row3", "soggiorno", "luminoso", System.currentTimeMillis(), Bytes.toBytes(false));
    	this.testTable.flushCommits();

    	result = this.client.get(this.testTable, "row3");    	
    	assertEquals(result.size(),2);
    	
    	this.testTable.setAutoFlush(true);
    }
    
    @Test
    public void hasInsertedRows() throws IOException {
    	
    	String[] rows = {"row4", "row4"};
    	String[] colfams = {"salone", "soggiorno"};
    	String[] cols = {"mq", "mq"};
    	long[] tss = {1L, 2L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	Result result = this.client.get(this.testTable, "row4");
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2);
    }
    
    @Test
    public void checkWriteBufferSize() throws IOException {
    	assertEquals(this.testTable.getWriteBufferSize(),20971520L);
    }
    
    @Test
    public void getAllVersions() throws IOException {
    	
    	String[] rows = {"row6", "row6", "row6"};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	String[] columnFamilies = {"salone"};
    	Result result = this.client.getHistory(this.testTable, "row6", columnFamilies);
    	
    	List<Integer> expected = new ArrayList<Integer>();
    	expected.add(11);
    	expected.add(10);

    	List<Integer> resultMq = new ArrayList<Integer>();
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(expected, resultMq);
    	
    	expected.add(12);
    	resultMq = new ArrayList<Integer>();
    	result = this.client.getHistory(this.testTable, "row6", null);
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(expected, resultMq); // since we had all qualifiers with the same name
    	
    	this.client.put(this.testTable, "row6", "soggiorno", "valore", 4L, Bytes.toBytes(40));
    	resultMq = new ArrayList<Integer>();
    	result = this.client.getHistory(this.testTable, "row6", null);
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertNotEquals(expected, resultMq); // since it adds up 40 to the list
    }
    
}
