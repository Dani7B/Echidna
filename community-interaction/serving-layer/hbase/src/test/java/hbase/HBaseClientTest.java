package hbase;

import hbase.entities.Room;
import hbase.impls.HBaseAdministratorImpl;
import hbase.impls.HBaseClientImpl;

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
    
    private HBaseAdministrator admin;
    
    private HBaseClient client;
        
    private HTable testTable;
    
    
    @BeforeClass
    public void setUp() throws IOException {
    	
    	this.admin = new HBaseAdministratorImpl();
		this.client = new HBaseClientImpl();

    	if(this.admin.existsTable("casa"))
    		this.admin.deleteTable("casa");
    	
    	this.admin.createTable("casa", "salone", "soggiorno", "cupola");
    	this.testTable = this.admin.getTable("casa");
    	this.testTable.setWriteBufferSize(20971520L); // Otherwise it doesn't get the default value from config
    
    	String row = "row5";
    	String[] rows = {row, row, row};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	String[] rowsDel = {"row13", "row14", "row13", "row15", "row15", "row16", "row16"};
    	String[] colfamsDel = {"salone", "soggiorno", "salone", "salone", "salone", "salone", "soggiorno"};
    	String[] colsDel = {"mq", "mq", "mq", "bright", "bright", "mq", "mq"};
    	long[] tssDel = {1L, 2L, 3L, 1L, 3L, 1L, 3L};
    	byte[][] valuesDel = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11),
    			Bytes.toBytes(true), Bytes.toBytes(false), Bytes.toBytes(20), Bytes.toBytes(23)};
    	this.client.put(this.testTable, rowsDel, colfamsDel, colsDel, tssDel, valuesDel);
    }
    
    
    @Test
    public void hasCreatedTable() throws IOException {
    	
    	this.admin.createTable("testing", "prova");
    	boolean value = this.admin.existsTable("testing");
    	assertTrue(value);
    	
    	HTable table = this.admin.getTable("testing");
    	assertNotNull(table);
    }
    
    
    @Test
    public void hasDeletedTable() throws IOException {
            
        this.admin.deleteTable("testing");
        boolean value = this.admin.existsTable("testing");
        assertFalse(value);
        
        HTable table = this.admin.getTable("testing");
        assertNull(table);
    }
    
    
    @Test
    public void hasInsertedRow() throws IOException {
    	
    	LOGGER.info("Auto flush: " + this.testTable.isAutoFlush());
    	
    	String row1 = "row1";
    	this.client.put(this.testTable, row1, "salone", "mq", System.currentTimeMillis(), Bytes.toBytes(25));
    	
    	Result result = this.client.get(this.testTable, row1);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    	
    	this.client.put(this.testTable, row1, "soggiorno", "luminoso", System.currentTimeMillis(), Bytes.toBytes(true));
    	result = this.client.get(this.testTable, row1);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2); //it's the size of the cell
    	
    	
    	String row2 = "row2";
    	this.client.put(this.testTable, row2, "soggiorno", "mq", System.currentTimeMillis(), Bytes.toBytes(16));
    	result = this.client.get(this.testTable, row2);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    }
    
    
    @Test
    public void hasInsertedRowWithoutAutoflush() throws IOException {
    	
    	this.testTable.setAutoFlush(false);
    	LOGGER.info("Auto flush: " + this.testTable.isAutoFlush());
    	
    	String row = "row3";
    	this.client.put(this.testTable, row, "soggiorno", "mq", System.currentTimeMillis(), Bytes.toBytes(15));
    	
    	Result result = this.client.get(this.testTable, row);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),0);
    	
    	this.client.put(this.testTable, row, "soggiorno", "luminoso", System.currentTimeMillis(), Bytes.toBytes(false));
    	this.testTable.flushCommits();

    	result = this.client.get(this.testTable, row);    	
    	assertEquals(result.size(),2);
    	
    	this.testTable.setAutoFlush(true);
    }
    
    
    @Test
    public void hasInsertedRows() throws IOException {
    	
    	String row = "row4";
    	String[] rows = {row, row};
    	String[] colfams = {"salone", "soggiorno"};
    	String[] cols = {"mq", "mq"};
    	long[] tss = {1L, 2L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(11)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	Result result = this.client.get(this.testTable, row);
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
    public void multipleGets() throws IOException {
    	String[] rows = {"row10", "row11", "row10"};
    	String[] colfams = {"salone", "soggiorno", "salone"};
    	String[] cols = {"mq", "mq", "mq"};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(16), Bytes.toBytes(21), Bytes.toBytes(17)};
    	this.client.put(this.testTable, rows, colfams, cols, tss, values);
    	
    	String[] rowsToQuery = {"row10", "row11"};
    	long[] timeRange = {1L, 4L};

    	List<Integer> mqs = new ArrayList<Integer>();
    	Result[] results = this.client.get(testTable, rowsToQuery, colfams, timeRange);
    	for(Result r : results) {
    		KeyValue[] kvs = r.raw();
    		for(KeyValue kv : kvs) {
    			mqs.add(Bytes.toInt(kv.getValue()));
    		}
    	}
    	
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(17, 21));
    	assertEquals(mqs, expected);
    }
    
    
    @Test
    public void putThenGetClass() throws IOException {
    	
    	String row = "row9";
    	Room dependance = new Room("dependance",35);
    	Room cucina = new Room("cucina",14);
    	
    	this.admin.createTable("house", "rooms");
    	HTable houseTable = this.admin.getTable("house");
    	
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
    
    @Test
    public void existsTest() throws IOException {
    	
    	String row = "row5";
    	boolean exists = this.client.exists(testTable, row);
    	assertTrue(exists);
    }

    @Test
    public void singleDeleteAllColumnsAllVersions() throws IOException {
    	
    	String row = "row16";
    	this.client.delete(testTable, row);
    	assertFalse(this.client.exists(testTable, row));
    }
    
    @Test
    public void singleDelete() throws IOException {
    	
    	String row = "row13";
    	long ts = 2L;
    	this.client.delete(testTable, row, "salone", "mq", ts);
    	assertTrue(this.client.exists(testTable, row));
    }
    
    @Test
    public void batchDelete() throws IOException {
    	
    	String[] rows = {"row15", "row14"};
    	String[] colfams = {"salone","soggiorno"};
    	String[] cols = {"bright", "mq"};
    	long[] tss = {5L, 5L};
    	this.client.delete(this.testTable, rows, colfams, cols, tss);
    	
    	for(String row : rows)
    		assertFalse(this.client.exists(testTable, row));
    }
    
}
