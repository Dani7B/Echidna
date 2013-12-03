package hbase;

import hbase.entities.Room;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

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
    	
    	this.client.createTable("casa", "salone", "soggiorno", "cupola");
    	this.testTable = this.client.getTable("casa");
    	this.testTable.setWriteBufferSize(20971520L); // Otherwise it doesn't get the default value from config
    
    	String row = "row5";
    	String[] rows = {row, row, row};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
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
    	
    	String row = "row5";
    	String[] columnFamilies = {"salone"};
    	Result result = this.client.getHistory(this.testTable, row, columnFamilies);
    	
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 10));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 2);
    	assertEquals(expected, resultMq);
    }
    
    @Test
    public void getAllVersionsAllColumnFamilies() throws IOException {
    	
    	String row = "row5";
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 10, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	Result result = this.client.getHistory(this.testTable, row, null);
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 3);
    	assertEquals(expected, resultMq);	
    }
 	
    @Test
    public void getAllVersionsColumnFamilyWithDifferentQualifier() throws IOException {
    	
    	String row = "row5";
    	List<Integer> expectedArray = new ArrayList<Integer>(Arrays.asList(5, 11, 10, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	this.client.put(this.testTable, row, "cupola", "raggio", 5L, Bytes.toBytes(5));
    	Result result = this.client.getHistory(this.testTable, row, null);
    	for(KeyValue kv : result.raw()) {
    		resultMq.add(Bytes.toInt(kv.getValue()));
    		LOGGER.info(Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier()) + " = "
    				+ Bytes.toInt(kv.getValue()));
    	}
    	
    	assertEquals(result.size(), 4);
    	assertEquals(resultMq, expectedArray);
    }
    	
    @Test
    public void getAllVersionsInTimeRange() throws IOException {
    	
    	String row = "row5";
    	long[] timeRange = {1L,5L};
    	Result result = this.client.getHistory(this.testTable, row, null, timeRange);
    	assertEquals(result.size(), 3);
    }
    
    @Test
    public void getLastVersion() throws IOException {
    	
    	String row = "row6";
    	String[] rows = {row, row, row};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	Result result = this.client.get(this.testTable, row);
    	
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 2);
    	assertEquals(expected, resultMq);
    }
    
    @Test
    public void getTimeRange() throws IOException {
    	
    	String row = "row7";
    	String[] rows = {row, row, row};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 4L, 8L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	String[] columnFamilies = {"salone"};
    	long[] timeRange = {1L,4L};
    	Result result = this.client.get(this.testTable, row, columnFamilies, timeRange);
    	
    	assertEquals(result.size(), 1);
    	
    	long[] timeRange2 = {1L,10L};
    	result = this.client.get(this.testTable, row, columnFamilies, timeRange2, 2);
    	
    	assertEquals(result.size(), 2);
    }
    
    @Test
    public void getTimeStamp() throws IOException {
    	
    	String row = "row8";
    	String[] rows = {row, row};
    	String[] colfams = {"salone", "salone"};
    	String[] cols = {"mq", "mq"};
    	long[] tss = {1L, 4L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	String[] columnFamilies = {"salone"};
    	Result result = this.client.get(this.testTable, row, columnFamilies, 4L);
    	
    	assertEquals(Bytes.toInt(result.getValue(Bytes.toBytes("salone"), Bytes.toBytes("mq"))), 12);
    }
    
    @Test
    public void putThenGetClass() throws IOException {
    	
    	String row = "row9";
    	Room dependance = new Room("dependance",35);
    	Room cucina = new Room("cucina",14);
    	
    	this.client.createTable("house", "rooms");
    	HTable houseTable = this.client.getTable("house");
    	
    	this.client.put(houseTable, row, "rooms", dependance.getName(), System.currentTimeMillis(),
    			Bytes.toBytes(dependance.getMq()));
    	this.client.put(houseTable, row, "rooms", cucina.getName(), System.currentTimeMillis(),
    			Bytes.toBytes(cucina.getMq()));
    	
    	Result result = this.client.get(houseTable, row);
    	    	
    	List<Room> house = new ArrayList<Room>();
    	
    	for(KeyValue kv : result.raw()) {
    		Room room = new Room(Bytes.toString(kv.getQualifier()), Bytes.toInt(kv.getValue()));
    		house.add(room);
    		LOGGER.info(room.toString());
    	}
    	
    	assertEquals(house.size(), 2);
    	
    	/* Alternative way to get to a specific cell.
    	 * Map&family,Map<qualifier,value>> */
    	NavigableMap<byte[],NavigableMap<byte[],byte[]>> resultMap = result.getNoVersionMap();
    	byte[] cucinaMq = resultMap.get(Bytes.toBytes("rooms")).get(Bytes.toBytes("cucina"));
    	
    	assertEquals(Bytes.toInt(cucinaMq),cucina.getMq());
    }
    

}
