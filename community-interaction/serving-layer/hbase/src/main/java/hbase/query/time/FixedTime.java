package hbase.query.time;

import hbase.HBaseClient;

public interface FixedTime {

	public abstract HBaseClient chooseHBaseClient();

	public abstract String generateRowKey(final long id);
}
