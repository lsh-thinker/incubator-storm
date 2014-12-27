package edu.sjtu.se.dclab;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class  GangliaCluster{
    private static final Logger LOG = LoggerFactory.getLogger(GangliaCluster.class);
	
	private String name;
	private Date localTime;
	
	Map<String, HostMetric> nameToHost;

	public GangliaCluster() {
		nameToHost = new HashMap<String, HostMetric> ();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, HostMetric> getNameToHost() {
		return nameToHost;
	}

	public Date getLocalTime() {
		return localTime;
	}

	public void setLocalTime(Date localTime) {
		this.localTime = localTime;
	} 
	
	
	public void print(){
		LOG.info("Cluster name=" + name);
		LOG.info(" LocalTime= " + localTime);
		for(Map.Entry<String, HostMetric> entry: nameToHost.entrySet()){
			entry.getValue().print();
		}
	}
	
	
}
