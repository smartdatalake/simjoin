package eu.smartdatalake.simjoin.simjoinservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.smartdatalake.simjoin.data.DataSource;
import eu.smartdatalake.simjoin.simjoinservice.models.nested.ThreadLog;

public class User {
	public String id;
	Map<String, DataSource> datasources;
	Map<String, String> datasourcesNames;
	Map<String, ThreadLog> threads;

	public User(String id) {
		this.id = id;
		datasources = new HashMap<String,DataSource>();
		datasourcesNames = new HashMap<String,String>();
		threads = new HashMap<String,ThreadLog>();
	}
	
	public List<String> getDataSources() {
		List<String> dataSources = new ArrayList<String>();
		for (Object key : datasourcesNames.keySet().toArray())
			dataSources.add(datasourcesNames.get(key) + ": " + key);
		return dataSources;
	}
	
	public void addThreadLog(String id, ThreadLog t) {
		threads.put(id, t);
		t.thread.start();
	}
	
	public ThreadLog getThreadLog(String id) {
		return threads.get(id);
	}
	
	public String getThreadStatus(String id) {
		return threads.get(id).thread.getState().toString();
	}
	
	public DataSource getDataSource(String id) {
		if (id == null)
			return null;
		return datasources.get(id);
	}
	
	public void addDataSource(String id, String name, DataSource ds) {
		datasources.put(id, ds);
		datasourcesNames.put(id, name);
	}
	
	public void removeDataSource(String id) {
		datasources.remove(id);
		datasourcesNames.remove(id);
	}
}
