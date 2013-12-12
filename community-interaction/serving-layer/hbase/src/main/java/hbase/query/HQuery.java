package hbase.query;

public class HQuery {
	
	private Authors users;
	
	private int amount;
	
	public HQuery() {
		this.users = new Authors(this);
	}
	
	public HQuery(HQuery q, int amount){
		this.amount = amount;
	}
	
	public Authors users() {
		return this.users;
	}
	
	public HQuery take(int amount) {
		this.amount = amount;
		return this;
	}
	
	public int getAmount() {
		return this.amount;
	}

}
