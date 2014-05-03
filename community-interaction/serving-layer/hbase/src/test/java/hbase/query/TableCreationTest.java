package hbase.query;

import java.io.IOException;

import hbase.HBaseAdministrator;
import hbase.impls.HTableAdmin;

/**
 * Simple class to create tables in HBase and assign them a coprocessor
 * @author Daniele Morgantini
 */
public class TableCreationTest {

    public static void main(String[] args) throws IOException {

        HBaseAdministrator admin = new HTableAdmin();
        String[] tables = new String[]{"mentionedBy", "mentionedByDay", "mentionedByMonth"};
        
        for(int i=0; i<tables.length; i++) {
        	if(admin.existsTable(tables[i])) {
        		admin.deleteTable(tables[i]);
            	System.out.println("Deleted table " + tables[i]);
        	}
        	admin.createTable(tables[i], "org.coprocessors.AuthorAggregatorEndpoint", "/home/daniele/coprocessors-1.0-SNAPSHOT.jar",
        			HBaseAdministrator.PRIORITY_USER, null, "t");
        	System.out.println("Created table " + tables[i]);
        }

    }
}
