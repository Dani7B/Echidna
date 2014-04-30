package hbase.query;

import java.io.IOException;

import hbase.HBaseAdministrator;
import hbase.impls.HTableAdmin;

/**
 * Simple class to create tables in HBase
 * @author Daniele Morgantini
 */
public class TableCreationTest {

    public static void main(String[] args) throws IOException {

        HBaseAdministrator admin = new HTableAdmin();
        
        admin.createTable("mentionedBy", "t");
        admin.createTable("mentionedByDay", "t");
        admin.createTable("mentionedByMonth", "t");

    }
}
