package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import java.sql.Timestamp;

public class ThreadLog {
	public int id;
	public Thread thread;
	public String output_file;
	public String log_file;
	public long time;
	public long limit;

	public ThreadLog(int id, Thread thread, long limit) {
		this.id = id;
		this.thread = thread;
		this.limit = limit;
		output_file = "output/" + id + ".txt";
		log_file = "logs/" + id + ".txt";
		time = System.currentTimeMillis();
	}

	public String toString() {
		return "Job: " + id + " created at " + new Timestamp(time);
	}
}
