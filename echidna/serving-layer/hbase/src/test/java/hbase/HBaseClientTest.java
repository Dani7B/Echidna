package hbase;

import hbase.entities.Room;
import hbase.impls.HTableAdmin;
import hbase.impls.HTableManager;

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
    
    private HTable testTable;
    
    private HTableManager casa;
    
    private byte[] salone;
    private byte[] soggiorno;
    private byte[] mq;
    private byte[] bright;
    
    @BeforeClass
    public void setUp() throws IOException {
    	
    	this.admin = new HTableAdmin();
    	
    	String testTableName = "casa";

    	if(this.admin.existsTable(testTableName))
    		this.admin.deleteTable(testTableName);
    	
    	this.admin.createTable(testTableName, "salone", "soggiorno", "cupola");
    	this.testTable = this.admin.getTable(testTableName);
    	this.testTable.setWriteBufferSize(20971520L); // Otherwise it doesn't get the default value from config
    	this.casa = new HTableManager(testTable,1);
    	this.salone = Bytes.toBytes("salone");
    	this.soggiorno = Bytes.toBytes("soggiorno");
    	this.bright = Bytes.toBytes("bright");
    	this.mq = Bytes.toBytes("mq");
    	
    	byte[] row = Bytes.toBytes("row5");
    	byte[][] rows = {row, row, row};
    	byte[][] colfams = {salone, soggiorno, salone};
    	byte[][] cols = {mq, mq, mq};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	byte[] row13 = Bytes.toBytes("row13");
    	byte[] row14 = Bytes.toBytes("row14");
    	byte[] row15 = Bytes.toBytes("row15");
    	byte[] row16 = Bytes.toBytes("row16");
    	byte[][] rowsDel = {row13, row14, row13, row15, row15, row16, row16};
    	byte[][] colfamsDel = {salone, soggiorno, salone, salone, salone, salone, soggiorno};
    	byte[][] colsDel = {mq, mq, mq, bright, bright, mq, mq};
    	long[] tssDel = {1L, 2L, 3L, 1L, 3L, 1L, 3L};
    	byte[][] valuesDel = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11),
    			Bytes.toBytes(true), Bytes.toBytes(false), Bytes.toBytes(20), Bytes.toBytes(23)};
    	this.casa.put(rowsDel, colfamsDel, colsDel, tssDel, valuesDel);
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
    	
    	byte[] row1 = Bytes.toBytes("row1");
    	this.casa.put(row1, salone, mq, System.currentTimeMillis(), Bytes.toBytes(25));
    	
    	Result result = this.casa.get(row1);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    	
    	this.casa.put(row1, soggiorno, Bytes.toBytes("luminoso"), System.currentTimeMillis(), Bytes.toBytes(true));
    	result = this.casa.get(row1);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2); //it's the size of the cell
    	
    	
    	byte[] row2 = Bytes.toBytes("row2");
    	this.casa.put(row2, soggiorno, mq, System.currentTimeMillis(), Bytes.toBytes(16));
    	result = this.casa.get(row2);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),1);
    }
    
    
    @Test
    public void hasInsertedRowWithoutAutoflush() throws IOException {
    	
    	this.testTable.setAutoFlush(false);
    	LOGGER.info("Auto flush: " + this.testTable.isAutoFlush());
    	
    	byte[] row = Bytes.toBytes("row3");
    	this.casa.put(row, soggiorno, mq, System.currentTimeMillis(), Bytes.toBytes(15));
    	
    	Result result = this.casa.get(row);    	
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),0);
    	
    	this.casa.put(row, soggiorno, Bytes.toBytes("luminoso"), System.currentTimeMillis(), Bytes.toBytes(false));
    	this.testTable.flushCommits();

    	result = this.casa.get(row);    	
    	assertEquals(result.size(),2);
    	
    	this.testTable.setAutoFlush(true);
    }
    
    
    @Test
    public void hasInsertedRows() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row4");
    	byte[][] rows = {row, row};
    	byte[][] colfams = {salone,soggiorno};
    	byte[][] cols = {mq, mq};
    	long[] tss = {1L, 2L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(11)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	
    	Result result = this.casa.get(row);
    	LOGGER.info(result.toString());
    	assertEquals(result.size(),2);
    }
    
    
    @Test
    public void checkWriteBufferSize() throws IOException {
    	assertEquals(this.testTable.getWriteBufferSize(),20971520L);
    }
    
    
    @Test
    public void getAllVersions() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row5");
    	byte[][] columnFamilies = {salone};
    	Result result = this.casa.getHistory(row, columnFamilies);
    	
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 10));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 2);
    	assertEquals(expected, resultMq);
    }
    
    
    @Test
    public void getAllVersionsAllColumnFamilies() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row5");
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 10, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	Result result = this.casa.getHistory(row, null);
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 3);
    	assertEquals(expected, resultMq);	
    }
 	
    
    @Test
    public void getAllVersionsColumnFamilyWithDifferentQualifier() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row5");
    	List<Integer> expectedArray = new ArrayList<Integer>(Arrays.asList(5, 11, 10, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	this.casa.put(row, Bytes.toBytes("cupola"), Bytes.toBytes("raggio"), 5L, Bytes.toBytes(5));
    	Result result = this.casa.getHistory(row, null);
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
    	
    	byte[] row = Bytes.toBytes("row5");
    	long[] timeRange = {1L,5L};
    	Result result = this.casa.getHistory(row, null, timeRange);
    	assertEquals(result.size(), 3);
    }
    
    
    @Test
    public void getLastVersion() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row6");
    	byte[][] rows = {row, row, row};
    	byte[][] colfams = {salone, soggiorno, salone};
    	byte[][] cols = {mq, mq, mq};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	
    	Result result = this.casa.get(row);
    	
    	List<Integer> expected = new ArrayList<Integer>(Arrays.asList(11, 12));
    	List<Integer> resultMq = new ArrayList<Integer>();
    	for(KeyValue kv : result.raw())
    		resultMq.add(Bytes.toInt(kv.getValue()));
    	
    	assertEquals(result.size(), 2);
    	assertEquals(expected, resultMq);
    }
    
    
    @Test
    public void getTimeRange() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row7");
    	byte[][] rows = {row, row, row};
    	byte[][] colfams = {salone, soggiorno, salone};
    	byte[][] cols = {mq, mq, mq};
    	long[] tss = {1L, 4L, 8L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12), Bytes.toBytes(11)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	
    	byte[][] columnFamilies = {salone};
    	long[] timeRange = {1L,4L};
    	Result result = this.casa.get(row, columnFamilies, timeRange);
    	
    	assertEquals(result.size(), 1);
    	
    	long[] timeRange2 = {1L,10L};
    	result = this.casa.get(row, columnFamilies, timeRange2, 2);
    	
    	assertEquals(result.size(), 2);
    }
    
    
    @Test
    public void getTimeStamp() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row8");
    	byte[][] rows = {row, row};
    	byte[][] colfams = {salone, salone};
    	byte[][] cols = {mq, mq};
    	long[] tss = {1L, 4L};
    	byte[][] values = {Bytes.toBytes(10), Bytes.toBytes(12)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	
    	byte[][] columnFamilies = {salone};
    	Result result = this.casa.get(row, columnFamilies, 4L);
    	
    	assertEquals(Bytes.toInt(result.getValue(salone, mq)), 12);
    }
    
    
    @Test
    public void multipleGets() throws IOException {
    	byte[] row10 = Bytes.toBytes("row10");
    	byte[] row11 = Bytes.toBytes("row11");
    	byte[][] rows = {row10, row11, row10};
    	byte[][] colfams = {salone, soggiorno, salone};
    	byte[][] cols = {mq, mq, mq};
    	long[] tss = {1L, 2L, 3L};
    	byte[][] values = {Bytes.toBytes(16), Bytes.toBytes(21), Bytes.toBytes(17)};
    	this.casa.put(rows, colfams, cols, tss, values);
    	
    	byte[][] rowsToQuery = {row10, row11};
    	long[] timeRange = {1L, 4L};

    	List<Integer> mqs = new ArrayList<Integer>();
    	Result[] results = this.casa.get(rowsToQuery, colfams, timeRange);
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
    	
    	byte[] row = Bytes.toBytes("row9");
    	byte[] rooms = Bytes.toBytes("rooms");
    	Room dependance = new Room("dependance",35);
    	Room cucina = new Room("cucina",14);
    	
    	this.admin.createTable("house", "rooms");
    	HTable houseTable = this.admin.getTable("house");
    	HTableManager house = new HTableManager(houseTable,1);
    	
    	house.put(row, rooms, Bytes.toBytes(dependance.getName()), System.currentTimeMillis(), 
    			Bytes.toBytes(dependance.getMq()));
    	house.put(row, rooms, Bytes.toBytes(cucina.getName()), System.currentTimeMillis(), Bytes.toBytes(cucina.getMq()));
    	
    	Result result = house.get(row);
    	    	
    	List<Room> roomList = new ArrayList<Room>();
    	
    	for(KeyValue kv : result.raw()) {
    		Room room = new Room(Bytes.toString(kv.getQualifier()), Bytes.toInt(kv.getValue()));
    		roomList.add(room);
    		LOGGER.info(room.toString());
    	}
    	
    	assertEquals(roomList.size(), 2);
    	
    	/* Alternative way to get to a specific cell.
    	 * Map&family,Map<qualifier,value>> */
    	NavigableMap<byte[],NavigableMap<byte[],byte[]>> resultMap = result.getNoVersionMap();
    	byte[] cucinaMq = resultMap.get(Bytes.toBytes("rooms")).get(Bytes.toBytes("cucina"));
    	
    	assertEquals(Bytes.toInt(cucinaMq),cucina.getMq());
    }
    
    @Test
    public void existsTest() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row5");
    	boolean exists = this.casa.exists(row);
    	assertTrue(exists);
    }

    @Test
    public void singleDeleteAllColumnsAllVersions() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row16");
    	this.casa.delete(row);
    	assertFalse(this.casa.exists(row));
    }
    
    @Test
    public void singleDelete() throws IOException {
    	
    	byte[] row = Bytes.toBytes("row13");
    	long ts = 2L;
    	this.casa.delete(row, salone, mq, ts);
    	assertTrue(this.casa.exists(row));
    }
    
    @Test
    public void batchDelete() throws IOException {
    	
    	byte[][] rows = {Bytes.toBytes("row15"), Bytes.toBytes("row14")};
    	byte[][] colfams = {salone, soggiorno};
    	byte[][] cols = {bright, mq};
    	long[] tss = {5L, 5L};
    	this.casa.delete(rows, colfams, cols, tss);
    	
    	for(byte[] row : rows)
    		assertFalse(this.casa.exists(row));
    }
    
}
