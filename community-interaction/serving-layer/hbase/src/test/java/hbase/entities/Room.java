package hbase.entities;

public class Room {
	
	private String name;
	private int mq;
	
	public Room(String name, int mq) {
		this.name = name;
		this.mq = mq;
	}
	
	public Room() {	}
	
	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public int getMq() {
		return mq;
	}

	public void setMq(final int mq) {
		this.mq = mq;
	}

	public String toString() {
		return this.name + " measures " + this.mq;
	}
}
