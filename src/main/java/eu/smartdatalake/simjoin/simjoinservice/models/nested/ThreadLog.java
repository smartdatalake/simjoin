package eu.smartdatalake.simjoin.simjoinservice.models.nested;

import java.sql.Timestamp;

import eu.smartdatalake.simjoin.runners.SimJoinRunner;

public class ThreadLog {
	public String id;
	public SimJoinRunner thread;
	public String output_file;
	public String log_file;
	public long time;
	public long limit;

	public ThreadLog(String id, SimJoinRunner thread, long limit) {
		this.id = id;
		this.thread = thread;
		this.limit = limit;
		output_file = "out/" + id + ".txt";
		log_file = "logs/" + id + ".txt";
		time = System.currentTimeMillis();
	}

	public String toString() {
		return "Job: " + id + " created at " + new Timestamp(time);
	}
}
